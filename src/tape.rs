// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:RFQ
// SPDX-License-Identifier: Apache-2.0

//! Anonymized RFQ tape + last-look analytics (DESIGN.md §4).
//!
//! Design `design/sentin8-exchange8-fx-technical-design.md` §3.5/§3.6:
//! - The RFQ tape is an **anonymized** public feed of quote activity -
//!   LP identities and client connection ids are stripped; only the
//!   pair, the two-way price, and the dealt side/qty survive. This lets
//!   market-data consumers see RFQ liquidity without leaking which LP
//!   quoted which price (anti-front-running, anti-quote-sniping).
//! - Last-look analytics aggregate per-LP reject behaviour (private):
//!   quotes sent vs last-look rejects vs late last-look answers, the
//!   inputs for the venue's LP scorecard.

use crate::engine::RfqEvent;
use std::collections::BTreeMap;

/// One anonymized tape entry (public feed shape).
///
/// `pair_id` is the numeric instrument id - symbols resolve client-side
/// via refdata; no LP or client identity ever appears.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TapeEntry {
    /// Monotonic sequence within the tape session.
    pub seq: u64,
    /// Microsecond timestamp (engine clock).
    pub ts_us: u64,
    /// Event kind on the tape.
    pub kind: TapeKind,
}

/// Anonymized tape event kinds.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TapeKind {
    /// An RFQ was broadcast (pair + size, no side - side would leak
    /// the client's intent and move the LP's quote).
    RfqBroadcast { pair_id: u64, qty: f64 },
    /// A two-way quote landed (anonymized: no lp identity).
    QuoteLanded { pair_id: u64, bid: f64, ask: f64 },
    /// A deal executed at the shown price.
    Traded { pair_id: u64, price: f64, qty: f64 },
    /// The RFQ expired without a trade.
    Expired { pair_id: u64 },
}

/// Internal context the tape needs per RFQ (kept engine-side, never
/// serialized): the pair and qty of each live RFQ.
#[derive(Clone, Debug, Default)]
pub struct RfqTape {
    seq: u64,
    entries: Vec<TapeEntry>,
}

impl RfqTape {
    pub fn new() -> Self {
        Self::default()
    }

    /// Project an engine event onto the tape. `pair_of` resolves the
    /// RFQ's pair (the tape keeps its own map so engine internals stay
    /// private). Returns the appended entry, if the event is tapeable.
    pub fn record(
        &mut self,
        ev: &RfqEvent,
        pair_of: &dyn Fn(u64) -> (u64, f64),
    ) -> Option<&TapeEntry> {
        let kind = match ev {
            RfqEvent::Requested { rfq_id, .. } => {
                let (pair_id, qty) = pair_of(*rfq_id);
                TapeKind::RfqBroadcast { pair_id, qty }
            }
            RfqEvent::Quoted {
                rfq_id, bid, ask, ..
            } => {
                let (pair_id, _) = pair_of(*rfq_id);
                // Tape shows the two-way only when both legs exist;
                // one-sided quotes stay off the public tape.
                match (bid, ask) {
                    (Some(b), Some(a)) => TapeKind::QuoteLanded {
                        pair_id,
                        bid: *b,
                        ask: *a,
                    },
                    _ => return None,
                }
            }
            RfqEvent::Traded {
                rfq_id, price, qty, ..
            } => {
                let (pair_id, _) = pair_of(*rfq_id);
                TapeKind::Traded {
                    pair_id,
                    price: *price,
                    qty: *qty,
                }
            }
            RfqEvent::Expired { rfq_id } => {
                let (pair_id, _) = pair_of(*rfq_id);
                TapeKind::Expired { pair_id }
            }
            // Rejected and LateQuote carry LP identity / reasons -
            // they are audit + analytics only, never tapeable.
            RfqEvent::Rejected { .. } | RfqEvent::LateQuote { .. } => return None,
        };
        self.seq += 1;
        self.entries.push(TapeEntry {
            seq: self.seq,
            ts_us: 0, // filled by the shell's clock at drain time
            kind,
        });
        self.entries.last()
    }

    /// All entries (snapshot for the SSE publisher).
    pub fn entries(&self) -> &[TapeEntry] {
        &self.entries
    }
}

/// Last-look analytics: per-LP scorecard inputs (private, admin-scoped).
///
/// Design §3.2 "last look bounded": the venue tracks how often each LP
/// exercises last look and how often it answers late - the reject-rate
/// gate for continued RFQ entitlement.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LastLookAnalytics {
    /// LP connection id → counters.
    per_lp: BTreeMap<String, LpStats>,
}

/// One LP's last-look scorecard.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LpStats {
    /// Hits parked at this LP awaiting last-look.
    pub hits_parked: u64,
    /// Last-look accepts (trade printed).
    pub accepted: u64,
    /// Last-look rejects (client's hit was declined).
    pub rejected: u64,
    /// Accepts that arrived after the LP's deadline (asymmetric -
    /// counted as trades but flagged for the scorecard).
    pub late_accepts: u64,
}

impl LastLookAnalytics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold engine events into the scorecard. LateQuote events are
    /// late *quotes* (window), not last-look answers - ignored here.
    pub fn record(&mut self, ev: &RfqEvent) {
        match ev {
            RfqEvent::Quoted { lp_conn, .. } => {
                // A quote that could be hit: no counter yet; parked is
                // counted at Traded/Rejected time via engine state.
                let _ = lp_conn;
            }
            RfqEvent::Traded { lp_conn, .. } => {
                let s = self.per_lp.entry(lp_conn.clone()).or_default();
                s.accepted += 1;
                s.hits_parked += 1;
            }
            RfqEvent::Rejected {
                lp_conn, reason, ..
            } => {
                let s = self.per_lp.entry(lp_conn.clone()).or_default();
                s.hits_parked += 1;
                if *reason == "last_look" {
                    s.rejected += 1;
                }
            }
            _ => {}
        }
    }

    /// Record a late last-look accept (engine counts `late_last_look`).
    pub fn record_late_accept(&mut self, lp_conn: &str) {
        let s = self.per_lp.entry(lp_conn.to_string()).or_default();
        s.late_accepts += 1;
    }

    /// The reject rate for an LP: rejected / hits_parked.
    /// None when the LP has never been hit.
    pub fn reject_rate(&self, lp_conn: &str) -> Option<f64> {
        let s = self.per_lp.get(lp_conn)?;
        if s.hits_parked == 0 {
            return None;
        }
        Some(s.rejected as f64 / s.hits_parked as f64)
    }

    /// Scorecard rows sorted by LP id (deterministic snapshot).
    pub fn snapshot(&self) -> Vec<(String, LpStats)> {
        self.per_lp
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tape_anonymizes_quotes_and_trades() {
        let mut tape = RfqTape::new();
        // pair 7 = EURUSD, 1M.
        let pair_of = |rfq_id: u64| (7u64, if rfq_id == 1 { 1_000_000.0 } else { 0.0 });
        tape.record(
            &RfqEvent::Requested {
                rfq_id: 1,
                lp_count: 3,
            },
            &pair_of,
        );
        tape.record(
            &RfqEvent::Quoted {
                rfq_id: 1,
                lp_conn: "LP-BIGBANK".into(),
                bid: Some(1.0845),
                ask: Some(1.0846),
            },
            &pair_of,
        );
        tape.record(
            &RfqEvent::Traded {
                rfq_id: 1,
                trade_id: 900,
                lp_conn: "LP-BIGBANK".into(),
                price: 1.0846,
                qty: 1_000_000.0,
            },
            &pair_of,
        );
        let es = tape.entries();
        assert_eq!(es.len(), 3);
        assert_eq!(es[0].seq, 1);
        assert_eq!(
            es[0].kind,
            TapeKind::RfqBroadcast {
                pair_id: 7,
                qty: 1_000_000.0
            }
        );
        assert_eq!(
            es[1].kind,
            TapeKind::QuoteLanded {
                pair_id: 7,
                bid: 1.0845,
                ask: 1.0846
            }
        );
        assert_eq!(
            es[2].kind,
            TapeKind::Traded {
                pair_id: 7,
                price: 1.0846,
                qty: 1_000_000.0
            }
        );
        // No entry mentions the LP or the client.
        let s = serde_json::to_string(&es).unwrap();
        assert!(!s.contains("LP-BIGBANK"));
        assert!(!s.contains("lp_conn"));
    }

    #[test]
    fn tape_drops_identity_events_and_one_sided_quotes() {
        let mut tape = RfqTape::new();
        let pair_of = |_: u64| (7u64, 0.0);
        // Rejected carries LP identity - never tapeable.
        assert!(tape
            .record(
                &RfqEvent::Rejected {
                    rfq_id: 1,
                    lp_conn: "LP-X".into(),
                    reason: "last_look"
                },
                &pair_of
            )
            .is_none());
        // Late quotes are audit-only.
        assert!(tape
            .record(
                &RfqEvent::LateQuote {
                    rfq_id: 1,
                    lp_conn: "LP-X".into()
                },
                &pair_of
            )
            .is_none());
        // One-sided quote (bid only) stays off the public tape.
        assert!(tape
            .record(
                &RfqEvent::Quoted {
                    rfq_id: 1,
                    lp_conn: "LP-X".into(),
                    bid: Some(1.0845),
                    ask: None,
                },
                &pair_of
            )
            .is_none());
        assert!(tape.entries().is_empty());
    }

    #[test]
    fn last_look_analytics_scorecard() {
        let mut a = LastLookAnalytics::new();
        for _ in 0..7 {
            a.record(&RfqEvent::Traded {
                rfq_id: 1,
                trade_id: 1,
                lp_conn: "LP-A".into(),
                price: 1.0,
                qty: 1.0,
            });
        }
        for _ in 0..3 {
            a.record(&RfqEvent::Rejected {
                rfq_id: 1,
                lp_conn: "LP-A".into(),
                reason: "last_look",
            });
        }
        a.record_late_accept("LP-A");
        let stats = &a.snapshot()[0].1;
        assert_eq!(stats.hits_parked, 10);
        assert_eq!(stats.accepted, 7);
        assert_eq!(stats.rejected, 3);
        assert_eq!(stats.late_accepts, 1);
        assert!((a.reject_rate("LP-A").unwrap() - 0.3).abs() < 1e-12);
        // An LP never hit has no rate.
        assert_eq!(a.reject_rate("LP-B"), None);
    }
}
