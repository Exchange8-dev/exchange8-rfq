// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:RFQ
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:RFQ service binary — FX quote-driven trading (FX design §3.2).
//!
//! Hosts the RFQ engine (`engine.rs`): client RFQs, LP quotes, last look,
//! streaming quotes. The engine is the deterministic core; this shell
//! wires health/registration/admin via the platform `sentin8-shell` crate
//! (see DESIGN.md §2, §10 — session wiring status).

#![allow(dead_code)]

// (see DESIGN.md §5). Workers (bus consumers, session
// supervisors, purge jobs) are in-process tokio tasks, so the
mod conformance;
mod engine;
mod registration;
mod openapi;
mod tape;

use std::sync::Arc;

/// Service state: shell core + the MemBus publish handle.
///
/// RFQ is a pub/sub-class publisher: `rfq.quote` (streaming LP quotes,
/// private to entitled sessions) and `rfq.event`
/// (anonymized tape events) — newest-wins families (see DESIGN.md §4).
pub struct RfqState {
    pub core: sentin8::shell::ServiceCore,
    pub membus: sentin8::membus::ServiceBus,
}

fn extend_router(
    app: axum::Router<std::sync::Arc<RfqState>>,
    _state: std::sync::Arc<RfqState>,
) -> axum::Router<std::sync::Arc<RfqState>> {
    // RFQ flow arrives via FIX 4.4 QuoteReq / WS sessions; the HTTP
    // router carries health + admin (DESIGN.md §2). Data endpoints land
    // with the RFW/handler pass like the sibling services.

    app
        // ── Sentin8:Policy admin API — shared implementation;
        // handlers generic over this state via the HasPolicyAdmin impl below;
        // axum 0.8 cannot merge a bound Router<()> into Router<std::sync::Arc<RfqState>>).
        .route(
            "/v1/admin/policy-sets",
            axum::routing::get(
                sentin8::shield::admin::list_policy_sets::<std::sync::Arc<RfqState>>,
            ),
        )
        .route(
            "/v1/admin/policy-sets/{name}",
            axum::routing::get(sentin8::shield::admin::get_policy_set::<std::sync::Arc<RfqState>>)
                .put(sentin8::shield::admin::put_policy_set::<std::sync::Arc<RfqState>>)
                .delete(sentin8::shield::admin::delete_policy_set::<std::sync::Arc<RfqState>>),
        )
        .route(
            "/v1/admin/policy-sets/{name}/test",
            axum::routing::post(
                sentin8::shield::admin::test_policy_set::<std::sync::Arc<RfqState>>,
            ),
        )
        .route(
            "/v1/admin/policy-sets/enforce",
            axum::routing::post(
                sentin8::shield::admin::admin_enforce_policy_set::<std::sync::Arc<RfqState>>,
            ),
        )
        .route(
            "/v1/admin/policy-sets/shadow",
            axum::routing::post(
                sentin8::shield::admin::admin_shadow_policy_set::<std::sync::Arc<RfqState>>,
            ),
        )
        .route(
            "/v1/admin/policy-sets/disable",
            axum::routing::post(
                sentin8::shield::admin::admin_disable_policy_set::<std::sync::Arc<RfqState>>,
            ),
        )
        .route(
            "/v1/admin/relations",
            axum::routing::get(sentin8::shield::admin::list_relations::<std::sync::Arc<RfqState>>)
                .put(sentin8::shield::admin::put_relation::<std::sync::Arc<RfqState>>)
                .delete(sentin8::shield::admin::delete_relation::<std::sync::Arc<RfqState>>),
        )
        .route(
            "/v1/admin/relations/check",
            axum::routing::get(sentin8::shield::admin::check_relation::<std::sync::Arc<RfqState>>),
        )
        .route(
            "/v1/admin/attributes",
            axum::routing::get(sentin8::shield::admin::list_attributes::<std::sync::Arc<RfqState>>)
                .put(sentin8::shield::admin::put_attribute::<std::sync::Arc<RfqState>>)
                .delete(sentin8::shield::admin::delete_attribute::<std::sync::Arc<RfqState>>),
        )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sentin8::shell::serve(
        sentin8::shell::Sentin8Shell::new("exchange8:rfq", "EXCHANGE8_RFQ")
            .with_registration(registration::print)
            .with_state(|core| async move {
                // RFQ publishes streaming quotes + the anonymized tape
                // (pub/sub class) onto the Memory Bus (DESIGN.md §2).
                let membus =
                    sentin8::membus::ServiceBus::connect("exchange8:rfq", &core.config.valkey_url)
                        .await;
                Ok(Arc::new(RfqState { core, membus }))
            })
            .with_router(extend_router),
    )
    .await
}

/// Shared policy-admin surface accessor: the shell boot
/// loaded the engine into `core.admin`; the shared handlers extract it via
/// `HasPolicyAdmin` so the admin surface and the request path evaluate the
/// SAME cache. (RfqState is bin-local, so the impl lives here.)
impl sentin8::shield::admin::HasPolicyAdmin for RfqState {
    fn policy_admin(&self) -> &sentin8::shield::admin::SharedAdminState {
        &self.core.admin
    }
}
