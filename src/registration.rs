// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:RFQ
// SPDX-License-Identifier: Apache-2.0

//! Service self-registration data for Exchange8:RFQ (FX quote-driven trading).
//!
//! Output via `--print-registration` (see the platform
//! `sentin8-service-self-registration-design.md`).

use utoipa::OpenApi;

use sentin8::registration::{
    AdminScope, AdminUi, AdminUiPage, CrossSchemaGrant, ErrorCode, M2mGrant, PgRole, PolicySetDecl,
    Registration, RelationDecl, SqlMigration,
};

/// The Shield text of the declared scope-mirror set - public so the
/// policy wiring tests (see DESIGN.md §9) assert against the REAL seed.
pub const SCOPE_MIRROR_POLICIES: &str = "permit(principal, action == Action::\"exchange8:rfq:quote\", resource) when { principal.scopes.contains(\"exchange8:rfq:quote\") };
permit(principal, action == Action::\"exchange8:rfq:quote\", resource) when { principal.scopes.contains(\"sentin8:sys:admin\") };
permit(principal, action == Action::\"exchange8:rfq:quote\", resource) when { principal.scopes.contains(\"sentin8:sys:org:admin\") };
permit(principal, action == Action::\"exchange8:rfq:quote\", resource) when { principal.scopes.contains(\"sentin8:sys:env:admin\") };";

/// Default declarative policy set (see DESIGN.md §4 §5; seeded by
/// `vertical enroll` as `origin='platform'`, `mode='enforced'`):
/// action-granular scope permits over the LIVE effective scope set - the
/// exact mirror of the gates this service's handlers evaluate in step 5
/// (Shield is the only gate; shadow is the emergency one-UPDATE rollback
/// lever only).
const POLICIES: &[PolicySetDecl] = &[PolicySetDecl {
    policy_set_name: "scope-mirror",
    description: "Action-granular scope permits for exchange8:rfq over the LIVE effective scope set. Enforced - step 5 is the only gate.",
    // Shield text lives in SCOPE_MIRROR_POLICIES (single source).
    policies: SCOPE_MIRROR_POLICIES,
    schema: None,
    mode: "enforced",
    default_enabled: true,
}];

/// Default ReBAC relation tuples (design §3.1c) - none declared by this
/// service today; env admins author their own graph via the relations
/// admin API.
const RELATIONS: &[RelationDecl] = &[];

const PG_ROLES: &[PgRole] = &[PgRole {
    scope: "exchange8:rfq:rfq:read,exchange8:rfq:rfq:write,exchange8:rfq:quote:write,exchange8:rfq:stream:read",
    role: "exchange8_rfq_service",
    bypass_rls: false,
        // Grants live in the exchange8 vertical database only (principles §6.6
        // database-per-vertical, schema-per-service).
        database: "exchange8",
        // Declared intra-database cross-schema reads (exactly these - never
        // blanket schema grants, principles §6.6a):
        cross_schema_grants: &[
        CrossSchemaGrant { schema: "refdata", table: "security", privilege: "SELECT" },
        CrossSchemaGrant { schema: "refdata", table: "currency", privilege: "SELECT" },
        ],
}];

const ADMIN_SCOPES: &[AdminScope] = &[
    AdminScope::Typed {
        role: "exchange8:admin",
        scope: "exchange8:rfq:rfq:read",
    },
    AdminScope::Typed {
        role: "exchange8:admin",
        scope: "exchange8:rfq:rfq:write",
    },
    AdminScope::Typed {
        role: "exchange8:admin",
        scope: "exchange8:rfq:quote:write",
    },
    AdminScope::Typed {
        role: "exchange8:admin",
        scope: "exchange8:rfq:stream:read",
    },
    AdminScope::Typed {
        role: "exchange8:admin",
        scope: "exchange8:fx:pairs:admin",
    },
    // Policy-admin surface (policy design §3.6; principles §6.0b per-service scope).
    AdminScope::Typed {
        role: "exchange8:rfq:admin",
        scope: "exchange8:rfq:policy:read",
    },
    AdminScope::Typed {
        role: "exchange8:rfq:admin",
        scope: "exchange8:rfq:policy:write",
    },
];

const ERROR_CODES: &[ErrorCode] = &[
    ErrorCode {
        enum_name: "Exchange8RfqNotFound",
        error_code: "exchange8_rfq_not_found",
        status_code: 404,
    },
    ErrorCode {
        enum_name: "Exchange8RfqQuoteLate",
        error_code: "exchange8_rfq_quote_late",
        status_code: 422,
    },
    ErrorCode {
        enum_name: "Exchange8RfqHitExpired",
        error_code: "exchange8_rfq_hit_expired",
        status_code: 422,
    },
];

const ADMIN_UI_PAGES: &[AdminUiPage] = &[
    AdminUiPage {
        route: "rfqs",
        label: "RFQs",
        ui_endpoint: "/v1/admin/ui/rfqs",
        data_endpoint: "/v1/admin/rfqs",
        icon: "0xe0af",
    },
    AdminUiPage {
        route: "pairs",
        label: "FX Pairs",
        ui_endpoint: "/v1/admin/ui/pairs",
        data_endpoint: "/v1/admin/pairs",
        icon: "0xe9ca",
    },
    AdminUiPage {
        route: "lp-sessions",
        label: "LP Sessions",
        ui_endpoint: "/v1/admin/ui/lp-sessions",
        data_endpoint: "/v1/admin/lp-sessions",
        icon: "0xe8b8",
    },
];

/// Workload scope grants (self-registration design §3.11). One entry per
/// workload this service presents a certificate for; `vertical enroll` upserts the
/// rows so the fail-closed registry map admits the registration. Scopes use the
/// full vocabulary form:
/// - `exchange8:rfq` / `rfq` - the service's own client (both
///   spellings granted; runtime call sites use the bare short name, the
///   map looks up the exact workload): bus produce/consume + the
///   shared certificate identity.
/// - `exchange8:rfq-policy` / `rfq-policy` - Shield hot-reload
///   consumer + stat emitter pair.
/// - `rfq-audit` - shell audit-journal pusher workload (§6.8; boot 8c).
/// - `exchange8-rfq-tls` - database-free HostContextResolver resolve workload
///   (TLS-RLS-4: `GET /v1/resolve/hostname`, scope
///   `sentin8:sys:instances:read`).
const M2M_GRANTS: &[M2mGrant] = &[
    M2mGrant {
        workload: "exchange8:rfq",
        scopes: &[
            "exchange8:rfq:rfq:read",
            "exchange8:rfq:rfq:write",
            "exchange8:rfq:quote:write",
            "exchange8:rfq:stream:read",
            "sentin8:bus:produce",
            "sentin8:bus:consume",
            "sentin8:stat:measure",
        ],
    },
    M2mGrant {
        workload: "rfq",
        scopes: &[
            "exchange8:rfq:rfq:read",
            "exchange8:rfq:rfq:write",
            "exchange8:rfq:quote:write",
            "exchange8:rfq:stream:read",
            "sentin8:bus:produce",
            "sentin8:bus:consume",
            "sentin8:stat:measure",
        ],
    },
    M2mGrant {
        workload: "exchange8:rfq-policy",
        scopes: &["sentin8:bus:consume", "sentin8:stat:measure"],
    },
    M2mGrant {
        workload: "rfq-policy",
        scopes: &["sentin8:stat:measure"],
    },
    M2mGrant {
        workload: "rfq-audit",
        scopes: &["sentin8:sys:audit:write"],
    },
    M2mGrant {
        workload: "exchange8-rfq-tls",
        scopes: &["sentin8:sys:instances:read"],
    },
];

const SQL_1_INIT: &str = include_str!("../sql/init.sql");

fn empty_openapi() -> serde_json::Value {
    serde_json::to_value(crate::openapi::RfqOpenApi::openapi()).unwrap_or(serde_json::Value::Null)
}

pub fn print(section: Option<String>) -> anyhow::Result<()> {
    let reg = Registration {
        service_type: "exchange8:rfq",
        subdomain: "exchange8-rfq",
        health_path: "/v1/health",
        admin_scopes: ADMIN_SCOPES,
        error_codes: ERROR_CODES,
        consent_providers: &[],
        consent_flows: &[],
        admin_ui: Some(AdminUi {
            label: "RFQ",
            icon: "0xe0af",
            service_flag: "exchange8-rfq",
            pages: ADMIN_UI_PAGES,
        }),
        sql_migrations: vec![SqlMigration {
            version: 1,
            name: "init",
            sql: SQL_1_INIT,
        }],
        pg_roles: PG_ROLES,
        audit_findings: None,
        service_actions: None,
        m2m_grants: Some(M2M_GRANTS),
        openapi_url: "/openapi.json",
        openapi: empty_openapi,
        policies: POLICIES,
        relations: RELATIONS,
        event_triggers: &[],
        schedules: &[],
    };
    sentin8::registration::print_registration(section, &reg, empty_openapi)
}
