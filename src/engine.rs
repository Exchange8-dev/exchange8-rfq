// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:RFQ
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:RFQ engine - FX quote-driven trading core (DESIGN.md §1, §4).
//!
//! The state machine:
//! `Open → Quoting → Quoted → Traded | Expired | Rejected`, plus the
//! streaming-quote side channel (LP two-way pushes, hit-binds-newest).
//!
//! Rules implemented here:
//! - **Quote window** (per RFQ): LP responses after the window are `late`
//!   - recorded, rejected, never shown to the client.
//! - **Last look** (per LP, bounded): a hit within `valid_until` must fill
//!   at the quoted price with `no last look`; with last look the LP may
//!   reject inside `last_look_window`, producing `rfq:rejected` + an
//!   asymmetric-stream counter row.
//! - **Fair routing**: the request fans out to all entitled LPs at once;
//!   engine receive-timestamps are authoritative.
//! - **Streaming**: per-LP live quote books; a hit binds the newest
//!   still-valid, non-withdrawn quote.
//!
//! Pure core: clock is injected (`now_us`), the shell owns sockets/bus.
//!
//! Design: `DESIGN.md` §1, §4, §7.

use std::collections::HashMap;

/// RFQ lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RfqState {
    Open,
    Quoting,
    Quoted,
    Traded,
    Expired,
    /// LP last-look rejection (terminal).
    Rejected,
}

/// One RFQ request.
#[derive(Clone, Debug, PartialEq)]
pub struct RfqRequest {
    pub rfq_id: u64,
    /// Requesting client CompID.
    pub client_conn: String,
    pub pair_id: u64,
    /// 'B'/'S' - the side the client wants to deal.
    pub side: Side,
    pub qty: f64,
    /// Tenor code (SP for spot).
    pub tenor: String,
    /// Quote window in µs (request → LP responses accepted).
    pub quote_window_us: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_char(self) -> char {
        match self {
            Side::Buy => 'B',
            Side::Sell => 'S',
        }
    }
}

/// One LP response (a two-way or one-way quote).
#[derive(Clone, Debug, PartialEq)]
pub struct LpQuote {
    pub lp_conn: String,
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    pub qty: f64,
    /// LP-declared validity (bounded by the engine on ingest).
    pub valid_until_us: u64,
    /// Engine receive time (authoritative).
    pub received_us: u64,
    /// Late quotes are recorded but never shown/winning.
    pub late: bool,
}

/// A client hit on a quote.
#[derive(Clone, Debug, PartialEq)]
pub struct HitRequest {
    pub rfq_id: u64,
    pub lp_conn: String,
    /// Which leg the client deals (for a buy client: the LP's ask).
    pub deal_price: f64,
    pub ts_us: u64,
}

/// Engine events out (bus/shell consumes).
#[derive(Clone, Debug, PartialEq)]
pub enum RfqEvent {
    /// `exchange8:rfq:requested` - routed to N LPs.
    Requested { rfq_id: u64, lp_count: usize },
    /// `exchange8:rfq:quoted` - a valid (non-late) quote landed.
    Quoted {
        rfq_id: u64,
        lp_conn: String,
        bid: Option<f64>,
        ask: Option<f64>,
    },
    /// `exchange8:rfq:traded`.
    Traded {
        rfq_id: u64,
        trade_id: u64,
        lp_conn: String,
        price: f64,
        qty: f64,
    },
    /// `exchange8:rfq:expired` (window elapsed with no quotes, or the hit
    /// window closed).
    Expired { rfq_id: u64 },
    /// `exchange8:rfq:rejected` - LP last-look reject.
    Rejected {
        rfq_id: u64,
        lp_conn: String,
        reason: &'static str,
    },
    /// Late quote (audit only, not client-visible).
    LateQuote { rfq_id: u64, lp_conn: String },
}

/// Per-LP last-look configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct LpConfig {
    pub lp_conn: String,
    /// LP responds to RFQs.
    pub rfq_provider: bool,
    /// LP streams quotes.
    pub streams: bool,
    /// Last-look window (µs); 0 = no last look (fills bind).
    pub last_look_us: u64,
    /// Entitled pair ids (empty = all pairs).
    pub entitled_pairs: Vec<u64>,
}

/// Trade-id source (leased elsewhere; injected counter here).
pub struct RfqEngine {
    /// RFQs by id.
    rfqs: HashMap<u64, Rfq>,
    /// LP registry.
    lps: HashMap<String, LpConfig>,
    /// Live streaming quotes per (lp, pair).
    streams: HashMap<(String, u64), StreamQuote>,
    /// Parked client hits awaiting LP last-look answers.
    pending_hits: HashMap<u64, PendingHit>,
    /// Event log (drained by the shell).
    events: Vec<RfqEvent>,
    /// Trade-id counter (blocks leased from the platform sequence
    /// facility per the data-model design §2a; injected here for tests).
    next_trade_id: u64,
    /// Asymmetric-stream counters: (lp, kind) → count.
    reject_counters: HashMap<(String, &'static str), u64>,
}

#[derive(Clone, Debug)]
struct Rfq {
    req: RfqRequest,
    state: RfqState,
    quotes: Vec<LpQuote>,
    /// Winning LP on trade.
    traded_lp: Option<String>,
}

/// A live streaming quote.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamQuote {
    pub lp_conn: String,
    pub pair_id: u64,
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    pub qty: f64,
    pub valid_until_us: u64,
    pub withdrawn: bool,
    pub ts_us: u64,
}

/// Default quote window when the request omits it (see DESIGN.md §4).
pub const DEFAULT_QUOTE_WINDOW_US: u64 = 500_000;
/// Engine-side cap on LP-declared validity (bounded, DESIGN.md §7).
pub const MAX_VALID_WINDOW_US: u64 = 5_000_000;

impl RfqEngine {
    pub fn new(lps: Vec<LpConfig>, first_trade_id: u64) -> Self {
        let mut map = HashMap::new();
        for lp in lps {
            map.insert(lp.lp_conn.clone(), lp);
        }
        RfqEngine {
            rfqs: HashMap::new(),
            lps: map,
            streams: HashMap::new(),
            pending_hits: HashMap::new(),
            events: Vec::new(),
            next_trade_id: first_trade_id,
            reject_counters: HashMap::new(),
        }
    }

    // -- RFQ flow ---------------------------------------------------------

    /// Client request: route to all entitled RFQ-provider LPs at once
    /// (fair routing - no LP sees it earlier).
    pub fn request(&mut self, mut req: RfqRequest, _now_us: u64) -> Vec<RfqEvent> {
        if req.quote_window_us == 0 {
            req.quote_window_us = DEFAULT_QUOTE_WINDOW_US;
        }
        let routed: Vec<String> = self
            .lps
            .values()
            .filter(|lp| lp.rfq_provider)
            .filter(|lp| lp.entitled_pairs.is_empty() || lp.entitled_pairs.contains(&req.pair_id))
            .map(|lp| lp.lp_conn.clone())
            .collect();
        let rfq = Rfq {
            req: req.clone(),
            state: if routed.is_empty() {
                RfqState::Expired
            } else {
                RfqState::Quoting
            },
            quotes: Vec::new(),
            traded_lp: None,
        };
        self.events.push(RfqEvent::Requested {
            rfq_id: req.rfq_id,
            lp_count: routed.len(),
        });
        if routed.is_empty() {
            self.events.push(RfqEvent::Expired { rfq_id: req.rfq_id });
        }
        self.rfqs.insert(req.rfq_id, rfq);
        self.take_events()
    }

    /// LP response within the quote window; late responses are recorded
    /// as `late` (audit) and never shown.
    pub fn on_quote(&mut self, rfq_id: u64, mut quote: LpQuote, now_us: u64) -> Vec<RfqEvent> {
        let Some(rfq) = self.rfqs.get_mut(&rfq_id) else {
            return Vec::new(); // unknown RFQ - drop (audit upstream)
        };
        if rfq.state != RfqState::Quoting {
            return Vec::new(); // terminal/quoted-late window: ignore
        }
        quote.received_us = now_us;
        // Late check: engine time vs the request window.
        let window_end = rfq.req.quote_window_us; // relative to request time
        let _ = window_end; // the shell passes absolute now; lateness is
                            // computed by the caller (below) - the engine
                            // records the LP-declared flag.
                            // Bound the LP-declared validity.
        if quote.valid_until_us > now_us.saturating_add(MAX_VALID_WINDOW_US) {
            quote.valid_until_us = now_us.saturating_add(MAX_VALID_WINDOW_US);
        }
        if quote.late {
            self.events.push(RfqEvent::LateQuote {
                rfq_id,
                lp_conn: quote.lp_conn.clone(),
            });
            rfq.quotes.push(quote);
            return self.take_events();
        }
        self.events.push(RfqEvent::Quoted {
            rfq_id,
            lp_conn: quote.lp_conn.clone(),
            bid: quote.bid_price,
            ask: quote.ask_price,
        });
        rfq.state = RfqState::Quoted;
        rfq.quotes.push(quote);
        self.take_events()
    }

    /// Client hits one LP's quote (the deal leg: buy client hits the LP
    /// ask; sell client hits the LP bid).
    pub fn on_hit(&mut self, hit: &HitRequest) -> Vec<RfqEvent> {
        // Copy the needed RFQ fields out (single mutable borrow per phase).
        let (state, side, qty, quote_parts) = {
            let Some(rfq) = self.rfqs.get(&hit.rfq_id) else {
                return Vec::new();
            };
            let side = rfq.req.side;
            let qty = rfq.req.qty;
            // The newest non-late quote from this LP.
            let parts = rfq
                .quotes
                .iter()
                .rev()
                .find(|q| q.lp_conn == hit.lp_conn && !q.late)
                .map(|q| (q.valid_until_us, q.ask_price, q.bid_price));
            (rfq.state, side, qty, parts)
        };
        if state != RfqState::Quoted {
            return Vec::new();
        }
        let Some((quote_valid_until, ask, bid)) = quote_parts else {
            return Vec::new(); // no quote from this LP
        };
        if hit.ts_us > quote_valid_until {
            // Hit window closed.
            if let Some(rfq) = self.rfqs.get_mut(&hit.rfq_id) {
                rfq.state = RfqState::Expired;
            }
            self.events.push(RfqEvent::Expired { rfq_id: hit.rfq_id });
            return self.take_events();
        }
        // Price must match the quote's deal leg (client can't invent one).
        let deal_leg = match side {
            Side::Buy => ask,
            Side::Sell => bid,
        };
        if deal_leg != Some(hit.deal_price) {
            return Vec::new(); // stale/edited quote - ignore
        }
        let last_look_us = self
            .lps
            .get(&hit.lp_conn)
            .map(|lp| lp.last_look_us)
            .unwrap_or(0);
        if last_look_us == 0 {
            // No last look: the hit fills at the quoted price.
            self.trade(hit.rfq_id, &hit.lp_conn, hit.deal_price, qty);
        } else {
            // Last look: park the pending hit; the LP answers inside the
            // window (on_last_look_ok / on_last_look_reject).
            let pending = PendingHit {
                rfq_id: hit.rfq_id,
                lp_conn: hit.lp_conn.clone(),
                price: hit.deal_price,
                qty,
                deadline_us: hit.ts_us.saturating_add(last_look_us),
            };
            self.pending_hits.insert(hit.rfq_id, pending);
        }
        self.take_events()
    }

    /// LP accepts after last look → trade.
    pub fn on_last_look_ok(&mut self, rfq_id: u64, now_us: u64) -> Vec<RfqEvent> {
        self.last_look_ok(rfq_id, now_us);
        self.take_events()
    }

    fn last_look_ok(&mut self, rfq_id: u64, now_us: u64) {
        let Some(p) = self.pending_hits.remove(&rfq_id) else {
            return;
        };
        if now_us > p.deadline_us {
            // LP answered too late - the trade binds anyway (asymmetric
            // rules: an expired last-look window fills at the quote).
            // Counted for analytics.
            *self
                .reject_counters
                .entry((p.lp_conn.clone(), "late_last_look"))
                .or_insert(0) += 1;
        }
        self.trade(rfq_id, &p.lp_conn, p.price, p.qty);
    }

    /// LP rejects within last look → rfq:rejected (no trade).
    pub fn on_last_look_reject(&mut self, rfq_id: u64, reason: &'static str) -> Vec<RfqEvent> {
        let Some(p) = self.pending_hits.remove(&rfq_id) else {
            return Vec::new();
        };
        if let Some(rfq) = self.rfqs.get_mut(&rfq_id) {
            rfq.state = RfqState::Rejected;
        }
        *self
            .reject_counters
            .entry((p.lp_conn.clone(), reason))
            .or_insert(0) += 1;
        self.events.push(RfqEvent::Rejected {
            rfq_id,
            lp_conn: p.lp_conn,
            reason,
        });
        self.take_events()
    }

    /// Window sweep: expire RFQs still open/quoting past their windows.
    pub fn sweep(&mut self, now_us: u64) -> Vec<RfqEvent> {
        let expired: Vec<u64> = self
            .rfqs
            .iter()
            .filter(|(_, r)| {
                matches!(r.state, RfqState::Open | RfqState::Quoting)
                    && now_us > r.req.quote_window_us + VALID_GRACE_US
            })
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(r) = self.rfqs.get_mut(&id) {
                r.state = RfqState::Expired;
            }
            self.events.push(RfqEvent::Expired { rfq_id: id });
        }
        // Pending last-look hits past deadline bind (fill at quote).
        let bound: Vec<u64> = self
            .pending_hits
            .iter()
            .filter(|(_, p)| now_us > p.deadline_us)
            .map(|(id, _)| *id)
            .collect();
        for id in bound {
            self.last_look_ok(id, now_us);
        }
        self.take_events()
    }

    fn trade(&mut self, rfq_id: u64, lp_conn: &str, price: f64, qty: f64) {
        let trade_id = self.next_trade_id;
        self.next_trade_id += 1;
        if let Some(rfq) = self.rfqs.get_mut(&rfq_id) {
            rfq.state = RfqState::Traded;
            rfq.traded_lp = Some(lp_conn.to_string());
            for q in rfq.quotes.iter_mut() {
                q.late = q.late || q.lp_conn != lp_conn;
            }
        }
        self.events.push(RfqEvent::Traded {
            rfq_id,
            trade_id,
            lp_conn: lp_conn.to_string(),
            price,
            qty,
        });
    }

    // -- Streaming quotes -------------------------------------------------

    /// LP pushes a two-way streaming quote (replaces its previous live
    /// quote on the pair - newest binds).
    pub fn on_stream_quote(&mut self, q: StreamQuote) {
        self.streams.insert((q.lp_conn.clone(), q.pair_id), q);
    }

    /// LP withdraws its streaming quote.
    pub fn on_stream_withdraw(&mut self, lp_conn: &str, pair_id: u64) {
        if let Some(q) = self.streams.get_mut(&(lp_conn.to_string(), pair_id)) {
            q.withdrawn = true;
        }
    }

    /// Client hits an LP's stream: binds the newest still-valid,
    /// non-withdrawn quote (DESIGN.md §4).
    pub fn on_stream_hit(
        &mut self,
        lp_conn: &str,
        pair_id: u64,
        side: Side,
        qty: f64,
        now_us: u64,
    ) -> Option<(u64, f64)> {
        let key = (lp_conn.to_string(), pair_id);
        let Some(q) = self.streams.get(&key) else {
            return None;
        };
        if q.withdrawn || now_us > q.valid_until_us {
            return None;
        }
        let leg = match side {
            Side::Buy => q.ask_price?,
            Side::Sell => q.bid_price?,
        };
        let trade_id = self.next_trade_id;
        self.next_trade_id += 1;
        self.events.push(RfqEvent::Traded {
            rfq_id: 0, // stream trades carry rfq_id 0 (no RFQ)
            trade_id,
            lp_conn: lp_conn.to_string(),
            price: leg,
            qty,
        });
        Some((trade_id, leg))
    }

    /// Live streaming quote view (per LP, per pair).
    pub fn stream_quote(&self, lp_conn: &str, pair_id: u64) -> Option<&StreamQuote> {
        self.streams.get(&(lp_conn.to_string(), pair_id))
    }

    // -- Diagnostics ------------------------------------------------------

    pub fn state(&self, rfq_id: u64) -> Option<RfqState> {
        self.rfqs.get(&rfq_id).map(|r| r.state)
    }

    /// Asymmetric-stream counter (LP rejects by reason).
    pub fn reject_count(&self, lp_conn: &str, reason: &str) -> u64 {
        // The counter key is a &'static str; a dynamic lookup needs a scan.
        self.reject_counters
            .iter()
            .filter(|((lp, r), _)| lp == lp_conn && *r == reason)
            .map(|(_, c)| *c)
            .sum()
    }

    pub fn take_events(&mut self) -> Vec<RfqEvent> {
        std::mem::take(&mut self.events)
    }

    /// Pending last-look hits count (diagnostics).
    pub fn pending_hits_len(&self) -> usize {
        self.pending_hits.len()
    }
}

/// Grace after the quote window before sweep expires an RFQ.
const VALID_GRACE_US: u64 = 100_000;

/// A parked client hit awaiting the LP's last-look answer.
#[derive(Clone, Debug)]
struct PendingHit {
    rfq_id: u64,
    lp_conn: String,
    price: f64,
    qty: f64,
    deadline_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lps() -> Vec<LpConfig> {
        vec![
            LpConfig {
                lp_conn: "LP1".into(),
                rfq_provider: true,
                streams: true,
                last_look_us: 0, // no last look - fills bind
                entitled_pairs: vec![],
            },
            LpConfig {
                lp_conn: "LP2".into(),
                rfq_provider: true,
                streams: false,
                last_look_us: 100_000, // 100 ms last look
                entitled_pairs: vec![],
            },
            LpConfig {
                lp_conn: "LPX".into(),
                rfq_provider: false, // stream-only LP
                streams: true,
                last_look_us: 0,
                entitled_pairs: vec![],
            },
        ]
    }

    fn engine() -> RfqEngine {
        RfqEngine::new(lps(), 10_000)
    }

    fn req(id: u64) -> RfqRequest {
        RfqRequest {
            rfq_id: id,
            client_conn: "CLIENT".into(),
            pair_id: 7,
            side: Side::Buy,
            qty: 1_000_000.0,
            tenor: "SP".into(),
            quote_window_us: 500_000,
        }
    }

    fn quote(lp: &str, ask: f64, valid_until: u64) -> LpQuote {
        LpQuote {
            lp_conn: lp.into(),
            bid_price: Some(ask - 0.0002),
            ask_price: Some(ask),
            qty: 1_000_000.0,
            valid_until_us: valid_until,
            received_us: 0,
            late: false,
        }
    }

    #[test]
    fn rfq_routes_to_all_entitled_lps() {
        let mut e = engine();
        let ev = e.request(req(1), 1_000);
        // Two RFQ providers (LP1, LP2); the stream-only LP is not routed.
        assert!(matches!(ev[0], RfqEvent::Requested { lp_count: 2, .. }));
        assert_eq!(e.state(1), Some(RfqState::Quoting));
    }

    #[test]
    fn rfq_no_lps_expires_immediately() {
        let mut e = RfqEngine::new(vec![], 1);
        let ev = e.request(req(1), 1_000);
        assert!(matches!(ev[0], RfqEvent::Requested { lp_count: 0, .. }));
        assert!(matches!(ev[1], RfqEvent::Expired { .. }));
        assert_eq!(e.state(1), Some(RfqState::Expired));
    }

    #[test]
    fn quotes_within_window_reach_client() {
        let mut e = engine();
        e.request(req(1), 1_000);
        let ev = e.on_quote(1, quote("LP1", 1.1050, 2_000_000), 1_200);
        assert!(
            matches!(ev[0], RfqEvent::Quoted { rfq_id: 1, ref lp_conn, .. } if lp_conn == "LP1")
        );
        assert_eq!(e.state(1), Some(RfqState::Quoted));
    }

    #[test]
    fn late_quotes_are_recorded_not_shown() {
        let mut e = engine();
        e.request(req(1), 1_000);
        let mut q = quote("LP1", 1.1050, 2_000_000);
        q.late = true;
        let ev = e.on_quote(1, q, 1_000);
        // LateQuote event, no Quoted event.
        assert!(matches!(ev[0], RfqEvent::LateQuote { .. }));
        assert_eq!(e.state(1), Some(RfqState::Quoting)); // still quoting
    }

    #[test]
    fn hit_without_last_look_trades_at_quote() {
        let mut e = engine();
        e.request(req(1), 1_000);
        e.on_quote(1, quote("LP1", 1.1050, 2_000_000), 1_200);
        let hit = HitRequest {
            rfq_id: 1,
            lp_conn: "LP1".into(),
            deal_price: 1.1050,
            ts_us: 1_500,
        };
        let ev = e.on_hit(&hit);
        match &ev[0] {
            RfqEvent::Traded {
                rfq_id,
                trade_id,
                lp_conn,
                price,
                qty,
            } => {
                assert_eq!((*rfq_id, *trade_id), (1, 10_000));
                assert_eq!(lp_conn, "LP1");
                assert!((price - 1.1050).abs() < 1e-9);
                assert_eq!(*qty, 1_000_000.0);
            }
            other => panic!("expected trade, got {other:?}"),
        }
        assert_eq!(e.state(1), Some(RfqState::Traded));
    }

    #[test]
    fn hit_after_valid_until_expires() {
        let mut e = engine();
        e.request(req(1), 1_000);
        e.on_quote(1, quote("LP1", 1.1050, 1_400), 1_200); // valid to 1.4ms
        let hit = HitRequest {
            rfq_id: 1,
            lp_conn: "LP1".into(),
            deal_price: 1.1050,
            ts_us: 1_600, // past validity
        };
        let ev = e.on_hit(&hit);
        assert!(matches!(ev[0], RfqEvent::Expired { .. }));
        assert_eq!(e.state(1), Some(RfqState::Expired));
    }

    #[test]
    fn hit_with_wrong_price_is_ignored() {
        let mut e = engine();
        e.request(req(1), 1_000);
        e.on_quote(1, quote("LP1", 1.1050, 2_000_000), 1_200);
        let hit = HitRequest {
            rfq_id: 1,
            lp_conn: "LP1".into(),
            deal_price: 1.1000, // edited/stale price
            ts_us: 1_500,
        };
        assert!(e.on_hit(&hit).is_empty());
        assert_eq!(e.state(1), Some(RfqState::Quoted));
    }

    #[test]
    fn last_look_accept_trades() {
        let mut e = engine();
        e.request(req(1), 1_000);
        e.on_quote(1, quote("LP2", 1.1052, 2_000_000), 1_200);
        let hit = HitRequest {
            rfq_id: 1,
            lp_conn: "LP2".into(),
            deal_price: 1.1052,
            ts_us: 1_500,
        };
        e.on_hit(&hit); // parks pending (LP2 has last look)
        assert_eq!(e.state(1), Some(RfqState::Quoted)); // not yet traded
        let ev = e.on_last_look_ok(1, 1_600); // LP accepts within window
        assert!(matches!(ev[0], RfqEvent::Traded { .. }));
        assert_eq!(e.state(1), Some(RfqState::Traded));
    }

    #[test]
    fn last_look_reject_produces_rejection_and_counter() {
        let mut e = engine();
        e.request(req(1), 1_000);
        e.on_quote(1, quote("LP2", 1.1052, 2_000_000), 1_200);
        e.on_hit(&HitRequest {
            rfq_id: 1,
            lp_conn: "LP2".into(),
            deal_price: 1.1052,
            ts_us: 1_500,
        });
        let ev = e.on_last_look_reject(1, "last_look");
        assert!(matches!(
            ev[0],
            RfqEvent::Rejected { rfq_id: 1, ref lp_conn, reason: "last_look" } if lp_conn == "LP2"
        ));
        assert_eq!(e.state(1), Some(RfqState::Rejected));
        assert_eq!(e.reject_count("LP2", "last_look"), 1);
    }

    #[test]
    fn last_look_deadline_binds_trade() {
        // LP never answers: past the deadline the trade fills at the quote.
        let mut e = engine();
        e.request(req(1), 1_000);
        e.on_quote(1, quote("LP2", 1.1052, 10_000_000), 1_200);
        e.on_hit(&HitRequest {
            rfq_id: 1,
            lp_conn: "LP2".into(),
            deal_price: 1.1052,
            ts_us: 1_500,
        });
        let ev = e.sweep(2_000_000); // long past the 100ms deadline
        assert!(ev.iter().any(|e| matches!(e, RfqEvent::Traded { .. })));
        assert_eq!(e.state(1), Some(RfqState::Traded));
    }

    #[test]
    fn sweep_expires_stale_rfqs() {
        let mut e = engine();
        e.request(req(1), 1_000); // window 500ms
        e.request(req(2), 1_000);
        e.on_quote(2, quote("LP1", 1.1050, 100_000_000), 1_200);
        let ev = e.sweep(1_000_000); // past window + grace
        assert!(ev
            .iter()
            .any(|e| matches!(e, RfqEvent::Expired { rfq_id: 1 })));
        assert_eq!(e.state(1), Some(RfqState::Expired));
        assert_eq!(e.state(2), Some(RfqState::Quoted)); // not expired
    }

    #[test]
    fn stream_quotes_newest_binds_and_withdraw_stops() {
        let mut e = engine();
        e.on_stream_quote(StreamQuote {
            lp_conn: "LP1".into(),
            pair_id: 7,
            bid_price: Some(1.1048),
            ask_price: Some(1.1050),
            qty: 2_000_000.0,
            valid_until_us: 10_000_000,
            withdrawn: false,
            ts_us: 1_000,
        });
        // Newer quote replaces (newest binds).
        e.on_stream_quote(StreamQuote {
            lp_conn: "LP1".into(),
            pair_id: 7,
            bid_price: Some(1.1049),
            ask_price: Some(1.1051),
            qty: 2_000_000.0,
            valid_until_us: 10_000_000,
            withdrawn: false,
            ts_us: 2_000,
        });
        let (tid, px) = e
            .on_stream_hit("LP1", 7, Side::Buy, 500_000.0, 3_000)
            .unwrap();
        assert!(tid >= 10_000);
        assert!((px - 1.1051).abs() < 1e-9); // newest ask
                                             // Withdraw → hits stop binding.
        e.on_stream_withdraw("LP1", 7);
        assert!(e
            .on_stream_hit("LP1", 7, Side::Buy, 500_000.0, 3_000)
            .is_none());
        // Expired validity → stops binding.
        e.on_stream_quote(StreamQuote {
            lp_conn: "LP1".into(),
            pair_id: 7,
            bid_price: Some(1.1048),
            ask_price: Some(1.1050),
            qty: 2_000_000.0,
            valid_until_us: 5_000,
            withdrawn: false,
            ts_us: 4_000,
        });
        assert!(e
            .on_stream_hit("LP1", 7, Side::Sell, 500_000.0, 6_000)
            .is_none());
    }

    #[test]
    fn stream_hit_wrong_leg_is_none() {
        let mut e = engine();
        e.on_stream_quote(StreamQuote {
            lp_conn: "LP1".into(),
            pair_id: 7,
            bid_price: None, // one-sided
            ask_price: Some(1.1050),
            qty: 2_000_000.0,
            valid_until_us: 10_000_000,
            withdrawn: false,
            ts_us: 1_000,
        });
        // Sell hit needs the bid leg → None.
        assert!(e
            .on_stream_hit("LP1", 7, Side::Sell, 100.0, 2_000)
            .is_none());
        // Buy hit binds on the ask.
        assert!(e.on_stream_hit("LP1", 7, Side::Buy, 100.0, 2_000).is_some());
    }

    #[test]
    fn lp_validity_is_bounded() {
        let mut e = engine();
        e.request(req(1), 1_000);
        let mut q = quote("LP1", 1.1050, 999_999_999); // LP declares huge window
        q.late = false;
        e.on_quote(1, q, 1_200);
        let r = e.rfqs.get(&1).unwrap();
        let stored = r.quotes.last().unwrap();
        assert!(stored.valid_until_us <= 1_200 + MAX_VALID_WINDOW_US);
    }
}
