// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:RFQ
// SPDX-License-Identifier: Apache-2.0

//! RFQ engine conformance scenarios (DESIGN.md §9).
//!
//! The RFQ lifecycle rules that every deployment must honor - quote
//! windows, late quotes, last-look binding, last-look rejects, and
//! streaming quote binding (design reference:
//! `design/sentin8-exchange8-fx-technical-design.md` §3.2). Each scenario drives the real [`RfqEngine`] through
//! a scripted clock and asserts the observable outcome, so the checks
//! double as the engine's conformance suite.

use crate::engine::{
    HitRequest, LpConfig, LpQuote, RfqEngine, RfqEvent, RfqRequest, RfqState, Side, StreamQuote,
};

/// Scenario driver: deterministic clock advanced manually.
pub struct Scenario {
    pub name: &'static str,
    pub now_us: u64,
}

impl Scenario {
    pub fn new(name: &'static str) -> Self {
        Scenario {
            name,
            now_us: 1_000,
        }
    }

    /// Advance the clock by `us`; returns the new now.
    pub fn tick(&mut self, us: u64) -> u64 {
        self.now_us += us;
        self.now_us
    }
}

/// Standard LP fixture set: one no-last-look RFQ LP, one last-look
/// RFQ LP, one stream-only LP.
pub fn standard_lps() -> Vec<LpConfig> {
    vec![
        LpConfig {
            lp_conn: "LP-DIRECT".into(),
            rfq_provider: true,
            streams: false,
            last_look_us: 0,
            entitled_pairs: vec![],
        },
        LpConfig {
            lp_conn: "LP-LL".into(),
            rfq_provider: true,
            streams: false,
            last_look_us: 200_000, // 200 ms last look
            entitled_pairs: vec![],
        },
        LpConfig {
            lp_conn: "LP-STREAM".into(),
            rfq_provider: false,
            streams: true,
            last_look_us: 0,
            entitled_pairs: vec![],
        },
    ]
}

fn engine() -> RfqEngine {
    RfqEngine::new(standard_lps(), 10_000)
}

fn req(rfq_id: u64, side: Side) -> RfqRequest {
    RfqRequest {
        rfq_id,
        client_conn: "CLIENT-1".into(),
        pair_id: 7,
        side,
        qty: 1_000_000.0,
        tenor: "SP".into(),
        quote_window_us: 500_000,
    }
}

fn quote(lp: &str, bid: f64, ask: f64, valid_until: u64, late: bool) -> LpQuote {
    LpQuote {
        lp_conn: lp.into(),
        bid_price: Some(bid),
        ask_price: Some(ask),
        qty: 1_000_000.0,
        valid_until_us: valid_until,
        received_us: 0,
        late,
    }
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

/// SCEN 1 - quote window: an RFQ left unanswered past window + grace
/// expires on sweep and emits `Expired`.
pub fn scen_quote_window_expiry() -> (String, bool) {
    let mut s = Scenario::new("quote_window_expiry");
    let mut eng = engine();
    // Routed to the two RFQ providers.
    eng.request(req(1, Side::Buy), s.now_us);
    // 500 ms window + 100 ms grace - nothing arrives.
    let now = s.tick(650_000);
    eng.sweep(now);
    let expired = eng.state(1) == Some(RfqState::Expired);
    (s.name.into(), expired)
}

/// SCEN 2 - late quotes never bind: a quote flagged late is recorded
/// for audit, never shown to the client.
pub fn scen_late_quote_never_wins() -> (String, bool) {
    let mut s = Scenario::new("late_quote_never_wins");
    let mut eng = engine();
    eng.request(req(1, Side::Buy), s.now_us);
    let now = s.tick(100_000);
    let evs = eng.on_quote(
        1,
        quote("LP-DIRECT", 1.0845, 1.0846, now + 400_000, true),
        now,
    );
    let late = evs
        .iter()
        .any(|e| matches!(e, RfqEvent::LateQuote { rfq_id, .. } if *rfq_id == 1));
    let quoted = evs
        .iter()
        .any(|e| matches!(e, RfqEvent::Quoted { rfq_id, .. } if *rfq_id == 1));
    // LateQuote observed, no Quoted event shown.
    (s.name.into(), late && !quoted)
}

/// SCEN 3 - last-look binding: the hit parks inside the LP's window
/// (no trade yet) and trades on `on_last_look_ok`, even when the LP
/// answers past its deadline (asymmetric late accept still binds).
pub fn scen_last_look_binding() -> (String, bool) {
    let mut s = Scenario::new("last_look_binding");
    let mut eng = engine();
    eng.request(req(1, Side::Buy), s.now_us);
    let now = s.tick(100_000);
    eng.on_quote(1, quote("LP-LL", 1.0845, 1.0846, now + 900_000, false), now);
    // Client hits the ask within validity.
    let hit_at = s.tick(50_000);
    let mid = eng.on_hit(&HitRequest {
        rfq_id: 1,
        lp_conn: "LP-LL".into(),
        deal_price: 1.0846,
        ts_us: hit_at,
    });
    // Nothing traded yet - parked for last look.
    let already = mid.iter().any(|e| matches!(e, RfqEvent::Traded { .. }));
    // LP answers past its 200 ms deadline (asymmetric late accept).
    let late_at = s.tick(300_000);
    let evs = eng.on_last_look_ok(1, late_at);
    let traded = evs
        .iter()
        .any(|e| matches!(e, RfqEvent::Traded { rfq_id, .. } if *rfq_id == 1));
    (s.name.into(), !already && traded)
}

/// SCEN 4 - last-look reject: the client's hit dies when the LP
/// rejects inside the window.
pub fn scen_last_look_reject() -> (String, bool) {
    let mut s = Scenario::new("last_look_reject");
    let mut eng = engine();
    eng.request(req(1, Side::Sell), s.now_us);
    let now = s.tick(100_000);
    eng.on_quote(1, quote("LP-LL", 1.0845, 1.0846, now + 900_000, false), now);
    let hit_at = s.tick(50_000);
    eng.on_hit(&HitRequest {
        rfq_id: 1,
        lp_conn: "LP-LL".into(),
        deal_price: 1.0845, // sell hits the bid
        ts_us: hit_at,
    });
    let evs = eng.on_last_look_reject(1, "last_look");
    let rejected = evs.iter().any(
        |e| matches!(e, RfqEvent::Rejected { rfq_id, reason: "last_look", .. } if *rfq_id == 1),
    );
    (s.name.into(), rejected)
}

/// SCEN 5 - streaming: the newest non-withdrawn valid stream quote
/// binds at the hit; a withdrawn quote cannot be hit.
pub fn scen_stream_binding() -> (String, bool) {
    let mut s = Scenario::new("stream_binding");
    let mut eng = engine();
    let t1 = s.tick(10_000);
    eng.on_stream_quote(StreamQuote {
        lp_conn: "LP-STREAM".into(),
        pair_id: 7,
        bid_price: Some(1.0844),
        ask_price: Some(1.0845),
        qty: 1_000_000.0,
        valid_until_us: t1 + 2_000_000,
        withdrawn: false,
        ts_us: t1,
    });
    // Newer quote supersedes (newest binds).
    let t2 = s.tick(10_000);
    eng.on_stream_quote(StreamQuote {
        lp_conn: "LP-STREAM".into(),
        pair_id: 7,
        bid_price: Some(1.0845),
        ask_price: Some(1.0846),
        qty: 1_000_000.0,
        valid_until_us: t2 + 2_000_000,
        withdrawn: false,
        ts_us: t2,
    });
    let hit_at = s.tick(10_000);
    let trade_new = eng.on_stream_hit("LP-STREAM", 7, Side::Buy, 500_000.0, hit_at);
    // Withdraw the stream; the next hit fails.
    eng.on_stream_withdraw("LP-STREAM", 7);
    let hit2 = s.tick(10_000);
    let trade_gone = eng.on_stream_hit("LP-STREAM", 7, Side::Buy, 500_000.0, hit2);
    // First hit trades at the newest ask (1.0846).
    let price_ok = trade_new
        .map(|(_, px)| (px - 1.0846).abs() < 1e-9)
        .unwrap_or(false);
    (s.name.into(), price_ok && trade_gone.is_none())
}

/// Run every scenario; returns (name, passed) rows.
pub fn run_all() -> Vec<(String, bool)> {
    vec![
        scen_quote_window_expiry(),
        scen_late_quote_never_wins(),
        scen_last_look_binding(),
        scen_last_look_reject(),
        scen_stream_binding(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_conformance_scenarios_pass() {
        let results = run_all();
        assert_eq!(results.len(), 5, "design §6 conformance families");
        for (name, passed) in &results {
            assert!(passed, "scenario {name} failed");
        }
    }
}
