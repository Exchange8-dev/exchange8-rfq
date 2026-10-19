-- SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
-- SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
-- SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:RFQ
-- SPDX-License-Identifier: Apache-2.0

-- Ex8:RFQ schema - FX pair master, tenor catalog, RFQ lifecycle, LP
-- streaming quotes, currency exposure (FX design §3.1/§3.2/§3.3, §4).
--
-- Applied by `sentin8-cli vertical init` (registered in
-- vertical-registry.json sql_dirs). RLS on every tenant table (FORCE).

CREATE SCHEMA IF NOT EXISTS rfq;

-- FX pair master (design §3.1). Extends refdata.security (security_type
-- 'Fx'); the pair metadata lives here.
CREATE TABLE IF NOT EXISTS rfq.fx_pair (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id          UUID NOT NULL,
    security_id     UUID NOT NULL REFERENCES refdata.security(id),
    base_currency   TEXT NOT NULL REFERENCES refdata.currency(code),
    quote_currency  TEXT NOT NULL REFERENCES refdata.currency(code),
    -- Pip size in raw price units at the pair's price_scale (e.g. 1 for
    -- 5-decimal pairs where a pip = 0.0001).
    pip_size        NUMERIC(18,8) NOT NULL DEFAULT 0.0001,
    -- Big-figure display digits.
    big_figure      INTEGER NOT NULL DEFAULT 4,
    -- Spot settlement convention.
    settlement      TEXT NOT NULL DEFAULT 't+2'
                    CHECK (settlement IN ('t+0','t+1','t+2')),
    -- Non-deliverable forward flag (cash-settled in quote ccy only).
    ndf             BOOLEAN NOT NULL DEFAULT FALSE,
    -- Which engines serve the pair (design §3.1 quote_type).
    quote_type      TEXT NOT NULL DEFAULT 'clob'
                    CHECK (quote_type IN ('clob','rfq','stream','clob+rfq')),
    -- Trading cut-off before the settlement cut (µs before cut, 0 = none).
    cutoff_us       BIGINT NOT NULL DEFAULT 0,
    UNIQUE (env_id, base_currency, quote_currency)
);
CREATE INDEX IF NOT EXISTS idx_exchange8_fxpair_env ON rfq.fx_pair (env_id);

-- Tenor catalog (SP = spot; 1W..12M forwards; value-date rule text).
CREATE TABLE IF NOT EXISTS rfq.fx_tenor (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    code        TEXT NOT NULL,             -- SP, 1W, 1M, 3M, 12M, ...
    days        INTEGER NOT NULL,          -- calendar days to value date
    rule        TEXT NOT NULL DEFAULT 'modified_following',
    UNIQUE (env_id, code)
);

-- RFQ lifecycle (design §3.2 state machine).
CREATE TABLE IF NOT EXISTS rfq.rfq (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id          UUID NOT NULL,
    rfq_id          BIGINT NOT NULL,       -- leased in-process (batch lease; design doc §2a)
    client_conn     TEXT NOT NULL,         -- requesting client CompID
    pair_id         UUID NOT NULL REFERENCES rfq.fx_pair(id),
    side            TEXT NOT NULL CHECK (side IN ('B','S')),
    qty             NUMERIC(24,8) NOT NULL,
    tenor           TEXT NOT NULL DEFAULT 'SP',
    -- State: Open → Quoting → Quoted → Traded/Expired/Rejected.
    state           TEXT NOT NULL DEFAULT 'open'
                    CHECK (state IN ('open','quoting','quoted','traded','expired','rejected')),
    requested_us    TIMESTAMPTZ NOT NULL DEFAULT now(),
    quote_window_us BIGINT NOT NULL DEFAULT 500_000,   -- 500 ms default
    terminal_us     TIMESTAMPTZ,
    trade_id        BIGINT,                -- set on traded
    UNIQUE (env_id, rfq_id)
);
CREATE INDEX IF NOT EXISTS idx_exchange8_rfq_env_state ON rfq.rfq (env_id, state);

-- LP responses to an RFQ.
CREATE TABLE IF NOT EXISTS rfq.rfq_quote (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id          UUID NOT NULL,
    rfq_id          BIGINT NOT NULL,
    lp_conn         TEXT NOT NULL,         -- LP CompID
    -- Two-way response.
    bid_price       NUMERIC(24,8),
    ask_price       NUMERIC(24,8),
    qty             NUMERIC(24,8) NOT NULL,
    -- Engine-received timestamp (authoritative; LP timestamps advisory).
    received_us     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Valid-until for the client hit window (LP-supplied, bounded).
    valid_until_us  TIMESTAMPTZ NOT NULL,
    -- Late = arrived after the quote window (rejected, not shown).
    late            BOOLEAN NOT NULL DEFAULT FALSE,
    -- Chosen quote when traded.
    winning         BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_exchange8_rfqquote_env_rfq ON rfq.rfq_quote (env_id, rfq_id);

-- Last-look rejection records (FX1.7 asymmetric-stream counters).
CREATE TABLE IF NOT EXISTS rfq.rfq_reject (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    rfq_id      BIGINT NOT NULL,
    lp_conn     TEXT NOT NULL,
    reason      TEXT NOT NULL,             -- last_look, stale, pulled...
    rejected_us TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_exchange8_rfqreject_env_lp ON rfq.rfq_reject (env_id, lp_conn);

-- LP streaming quotes (live two-way pushes, private per LP session).
CREATE TABLE IF NOT EXISTS rfq.lp_stream_quote (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    lp_conn     TEXT NOT NULL,
    pair_id     UUID NOT NULL REFERENCES rfq.fx_pair(id),
    bid_price   NUMERIC(24,8),
    ask_price   NUMERIC(24,8),
    qty         NUMERIC(24,8) NOT NULL,
    valid_until_us TIMESTAMPTZ NOT NULL,
    -- Withdrawn quotes keep the row (audit) but stop binding.
    withdrawn   BOOLEAN NOT NULL DEFAULT FALSE,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (env_id, lp_conn, pair_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_exchange8_lpstream_env_pair ON rfq.lp_stream_quote (env_id, pair_id, lp_conn);

-- Per-currency net exposure + limit (FX1.9, design §3.3).
CREATE TABLE IF NOT EXISTS rfq.currency_exposure (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    currency    TEXT NOT NULL REFERENCES refdata.currency(code),
    -- Net position in the currency (signed, quote-ccy notional units).
    net_position NUMERIC(24,8) NOT NULL DEFAULT 0,
    exposure_limit NUMERIC(24,8) NOT NULL DEFAULT 0, -- 0 = unlimited
    value_date   DATE,                    -- forwards bucket by date; NULL spot
    UNIQUE (env_id, currency, value_date)
);
CREATE INDEX IF NOT EXISTS idx_exchange8_curexposure_env ON rfq.currency_exposure (env_id, currency);

-- ---------------------------------------------------------------------------
-- RLS (architecture-principles.md §4.3): enable + force on all tenant tables.
-- ---------------------------------------------------------------------------
DO $$
DECLARE tbl TEXT;
BEGIN
    FOREACH tbl IN ARRAY ARRAY['rfq.fx_pair', 'rfq.fx_tenor', 'rfq.rfq', 'rfq.rfq_quote', 'rfq.rfq_reject', 'rfq.lp_stream_quote', 'rfq.currency_exposure'] LOOP
        BEGIN
            EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
            EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_select ON %2$s;
CREATE POLICY %1$I_select ON %2$s FOR SELECT USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_insert ON %2$s;
CREATE POLICY %1$I_insert ON %2$s FOR INSERT WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_update ON %2$s;
CREATE POLICY %1$I_update ON %2$s FOR UPDATE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_delete ON %2$s;
CREATE POLICY %1$I_delete ON %2$s FOR DELETE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'RLS skip %', tbl;
        END;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- Per-service role grants (schema-per-service: this service owns schema
-- `rfq`; grants live in db `exchange8` only - design doc §1).
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'exchange8_rfq_service') THEN
        ALTER ROLE exchange8_rfq_service SET search_path = rfq, public;
        GRANT USAGE ON SCHEMA rfq TO exchange8_rfq_service;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA rfq TO exchange8_rfq_service;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA rfq TO exchange8_rfq_service;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Local outbox (DBX §6.7 - generated from `sentin8_outbox::outbox_table_sql(
-- "rfq")`): rows written in the same DB transaction as the domain change;
-- the shell's relay (boot step 8b) pushes them to Bus `POST /v1/produce`.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rfq.outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id          UUID NOT NULL,
    event_type      TEXT NOT NULL,
    stream_family   TEXT NOT NULL,
    payload         JSONB NOT NULL,
    publisher_id_token  TEXT,
    publisher_subject   TEXT,
    relayed         BOOLEAN NOT NULL DEFAULT false,
    relayed_at      TIMESTAMPTZ,
    relay_attempts  INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_rfq_outbox_unrelayed
    ON rfq.outbox (created_at) WHERE relayed = false;
DO $$ BEGIN
    EXECUTE 'ALTER TABLE rfq.outbox ENABLE ROW LEVEL SECURITY';
    EXECUTE 'ALTER TABLE rfq.outbox FORCE ROW LEVEL SECURITY';
    EXECUTE 'DROP POLICY IF EXISTS outbox_select ON rfq.outbox';
    EXECUTE 'CREATE POLICY outbox_select ON rfq.outbox FOR SELECT
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
    EXECUTE 'DROP POLICY IF EXISTS outbox_insert ON rfq.outbox';
    EXECUTE 'CREATE POLICY outbox_insert ON rfq.outbox FOR INSERT
        WITH CHECK (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
    EXECUTE 'DROP POLICY IF EXISTS outbox_update ON rfq.outbox';
    EXECUTE 'CREATE POLICY outbox_update ON rfq.outbox FOR UPDATE
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'RLS setup failed for rfq.outbox: %', SQLERRM;
END $$;

CREATE OR REPLACE FUNCTION rfq.rfq_notify_outbox() RETURNS TRIGGER
LANGUAGE plpgsql AS $fn$
BEGIN
    PERFORM pg_notify('rfq_outbox_changed', '');
    RETURN NEW;
END;
$fn$;
DROP TRIGGER IF EXISTS outbox_notify_trigger ON rfq.outbox;
CREATE TRIGGER outbox_notify_trigger
    AFTER INSERT ON rfq.outbox
    FOR EACH ROW
    EXECUTE FUNCTION rfq.rfq_notify_outbox();

-- ---------------------------------------------------------------------------
-- Local audit journal (DBX §6.8 - generated from
-- `sentin8_audit::audit_journal_sql("rfq")`): rows written in the same
-- transaction as the domain change; the shell's pusher (boot step 8c) pushes
-- them to Sys `POST /v1/audit` (`sentin8:sys:audit:write`). A Sys outage
-- never drops audit.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rfq.audit_journal (
    id              UUID PRIMARY KEY,
    env_id          UUID NOT NULL,
    action          TEXT NOT NULL,
    target_resource_type TEXT,
    target_resource_id    TEXT,
    actor_user_id   TEXT,
    actor_subject_type TEXT,
    severity        TEXT NOT NULL DEFAULT 'info',
    correlation_id  TEXT,
    before_state    JSONB,
    after_state     JSONB,
    details         JSONB,
    pushed          BOOLEAN NOT NULL DEFAULT false,
    pushed_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_rfq_audit_journal_unpushed
    ON rfq.audit_journal (created_at) WHERE pushed = false;
DO $$ BEGIN
    EXECUTE 'ALTER TABLE rfq.audit_journal ENABLE ROW LEVEL SECURITY';
    EXECUTE 'ALTER TABLE rfq.audit_journal FORCE ROW LEVEL SECURITY';
    EXECUTE 'DROP POLICY IF EXISTS audit_journal_dml ON rfq.audit_journal';
    EXECUTE 'CREATE POLICY audit_journal_dml ON rfq.audit_journal
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())
        WITH CHECK (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'RLS setup failed for rfq.audit_journal: %', SQLERRM;
END $$;

-- Outbox + journal role grants (same pattern as the table grants above).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'exchange8_rfq_service') THEN
        GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA rfq TO exchange8_rfq_service;
    END IF;
END $$;
