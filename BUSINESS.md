# Exchange8:RFQ - Business Requirements & Functionality

> Audience: product owners, business analysts, and anyone evaluating what
> Exchange8:RFQ **does for the business**. For architecture read `README.md`;
> for wire-level detail read `DESIGN.md`.

## 1. Business Purpose

- Open the exchange's capital-markets platform to **quote-driven FX
  dealing**: the institutional RFQ model in which a client requests prices
  from all eligible liquidity providers at once, compares two-way quotes,
  and deals with the one it chooses.
- Guarantee **market integrity in the window game**: every RFQ operates
  under bounded, venue-authoritative timing - a quote window, per-quote
  validity, and a bounded LP last look - so neither side can bank on unfair
  timing.
- Protect the client's price certainty: once a hit is accepted, the deal
  prints **at the quoted price** - the client can never be repriced by an
  LP's hesitation.
- Publish an **anonymized tape** of RFQ activity so the market can see
  liquidity flow without exposing who quoted what to whom (anti
  front-running, anti quote-sniping).
- Make LP behaviour **measurable and governable**: last-look rejects and
  late answers feed per-LP scorecards, which underpin the entitlement gate
  the venue applies to its liquidity providers.
- Serve pairs by model: the venue routes FX between the central-limit
  order book and this quote-driven engine per pair, so liquidity providers
  participate where their model fits.
- Hold the **FX pair knowledge** the venue operates on - pip sizes, big
  figures, settlement conventions, NDF flags, tenors and per-pair cutoffs -
  as first-class reference data for the FX model.

## 2. Business Scope (What the Service Provides)

| # | Capability | Description |
|---|---|---|
| 1 | RFQ request & fair routing | Client RFQs (pair, side, qty, tenor) fan out to all entitled LPs simultaneously; no LP sees the request earlier |
| 2 | Quote collection & compare | Two-way LP quotes inside a bounded window; late quotes are recorded but never shown |
| 3 | Hit & deal binding | The client hits one quote at exactly the shown price; hits outside validity expire the RFQ |
| 4 | Last look | Bounded per-LP accept/reject windows; rejected hits print the reason and count for scorecards; late accepts still fill at the quote |
| 5 | Streaming quotes | LPs push two-way prices continuously per pair; a hit binds the newest valid, non-withdrawn quote |
| 6 | Anonymized tape | Public RFQ tape: pair, two-way price, dealt size - no LP/client identity, no one-sided quotes |
| 7 | LP scorecards | Per-LP counters (hits parked, accepts, rejects by reason, late accepts) with a derived reject rate |
| 8 | FX pair & tenor master | Pair metadata on top of the security master: pip size, big figure, settlement (T+0/T+1/T+2), NDF flag, serving model per pair, cutoff; tenor catalog (SP, 1W..12M) with value-date rules |
| 9 | Currency exposure records | Net position and limit per currency (with value-date buckets for forwards) |
| 10 | Admin surfaces | Three RFW admin pages (RFQs, FX pairs, LP sessions) plus the Shield policy-admin API |
| 11 | Events & audit | Every state change journaled locally, relayed to Sentin8:Bus (outbox producer), pushed to the platform audit ingest |

**What the service does NOT own (business boundary)**

| Concern | Owner |
|---|---|
| Central-limit order book for pair trading | Exchange8:ME |
| Client/LP session transports (FIX 4.4, WebSocket) and codec | Exchange8:Gateway |
| Per-currency limit policy decisions | Exchange8:Risk |
| Position ledger, novation, FX settlement legs | Exchange8:Clearing / Exchange8:Settlement |
| Fiat money movement itself | Bank8:Ledger |
| Security master and currency catalog | Exchange8:RefData |
| Consumer-facing market-data products (SSE feeds, book data) | Exchange8:MD / Exchange8:VFeeds |
| Cron schedules (cut-off sweeps as jobs) | Sentin8:Cron |
| Identity, tokens, M2M client registration | Sentin8:Auth |
| Message delivery between services | Sentin8:Bus |

## 3. Business Capabilities

### RFQ dealing

| Capability | Business rule |
|---|---|
| Request routing | Every RFQ fans out to all LPs entitled for the pair at the same moment; an RFQ with no entitled LP expires immediately |
| Quote window | Responses are accepted only inside the RFQ's window (default 500 ms); a late response is recorded for audit and is invisible to the client |
| Two-way pricing | LPs respond with a two-way (or one-sided) quoted price at size; the venue never re-prices or re-scales an LP's price |
| Quoted price certainty | The client deals only at a price actually shown; the venue rejects deal attempts at any other level |
| Hit validity | Each quote carries its own valid-until; a hit past validity expires the RFQ - the price is pulled |
| Last look | An LP may exercise a bounded last look: an accept binds the trade at the quoted price (even a late accept); a reject within the window declines the deal and is recorded with its reason |
| Lifecycle finality | Every RFQ ends in exactly one of traded / expired / rejected, recorded with its terminal state and (when traded) the winning LP |

### Streaming quotes & tape

| Capability | Business rule |
|---|---|
| Continuous availability | Entitled LPs may serve pairs by continuously streaming two-way prices, no RFQ required |
| Newest binds | A streaming hit binds the LP's latest still-valid quote; a newer push automatically supersedes the previous one |
| Withdrawal | An LP's withdrawal takes its quote off immediately; withdrawn quotes can never be hit |
| Public tape | The venue publishes an anonymized record of RFQ broadcasts, quoted prices, trades and expiries - pairs and prices only |
| No intent leakage | The tape shows the RFQ's size but not its side, and never shows which LP quoted which price or which client dealt |

### LP behaviour management

| Capability | Business rule |
|---|---|
| Scorecards | Each LP's response behaviour - quotes sent, deals accepted, deals rejected by reason, late answers - is measured continuously |
| Reject accountability | Every last-look rejection is persisted with its reason and attributed to the LP |
| Late-answer accountability | An LP that answers last look after its own deadline still must fill at the quoted price; the lateness is counted |
| Entitlement review inputs | Reject rates per LP are the business input for keeping or withdrawing RFQ/streaming entitlement |

### FX reference data & exposure

| Capability | Business rule |
|---|---|
| Pair model | Every pair is defined with base/quote currency, pip size, big-figure digits, settlement convention (T+0/T+1/T+2), NDF flag, serving model (`clob` / `rfq` / `stream` / `clob+rfq`) and optional trading cutoff |
| Tenor model | Spot (`SP`) and forward tenors (1W..12M) with calendar-day offsets and a value-date rule |
| Currency exposure | The venue tracks its net signed position per currency (bucketed by value date for forwards) with an optional exposure limit (0 = unlimited) |
| Pair uniqueness | One pair definition per environment per currency pair - no duplicate instrument definitions |

## 4. Business Events (Outbound)

| Event (Sentin8:Bus family) | Fires when |
|---|---|
| `rfq:requested` | An RFQ has been routed to its entitled LPs (carries the fan-out size) |
| `rfq:quoted` | A valid (non-late) LP quote has landed on an RFQ |
| `rfq:traded` | A client hit has bound at the quoted price (RFQ deal or streaming deal) |
| `rfq:expired` | The quote window elapsed without answers, or the hit's validity passed |
| `rfq:rejected` | An LP exercised last look and declined the deal (reason attached) |
| `rfq:late quote` (audit) | A quote arrived after the window - audit-only, never client-visible |
| `stream:quote` / `stream:withdrawn` | An LP pushed or pulled its streaming two-way price |

MemBus (newest-wins, session-private) families: `rfq.quote` for live LP
streaming quotes; `rfq.event` for the anonymized tape.

**Inbound (consumed context):** client and LP session events arrive via
Exchange8:Gateway session transports; reference data (`refdata.security`,
`refdata.currency`, pair/tenor masters) is read under declared grants;
platform events (`sentin8:sys:*` families, Shield policy updates) ride the
standard shell consumers.

## 5. Business Rules & Invariants

1. Every RFQ request routes to **all** entitled LPs simultaneously -
   selective or sequential routing is not permitted by design.
2. Engine receive time decides all windows; supplier-declared timestamps
   are never authoritative.
3. A quote arriving after the RFQ's quote window can never be shown to or
   traded by the client; it exists only as an audit record and a scorecard
   input.
4. An LP's quoted validity is capped by the venue; no quote can carry
   unbounded validity.
5. The client can deal only at a price the engine showed for that leg -
   invented, edited or stale prices never bind.
6. A deal past a quote's validity expires the RFQ; the quote is pulled.
7. Last look is bounded and asymmetric: an accept - even a late one - fills
   at the quoted price; a reject within the window declines the deal and is
   measured. Reject reasons are always known and recorded.
8. A streaming hit binds the LP's **newest** valid, non-withdrawn quote;
   withdrawn or expired quotes bind nothing, and a one-sided quote cannot
   serve the missing leg.
9. RFQ trades and streaming trades share one trade-id sequence - the
   business trade count is venue-complete.
10. The public tape never identifies an LP, a client or a one-sided quote,
    and never reveals the dealt side of a broadcast.
11. Every RFQ state change is durably recorded with the domain write and
    its outbound event in one atomic transaction; audit rows survive any
    downstream outage.
12. Pair serving model is declared reference data: this engine serves pairs
    marked `rfq` / `stream` / `clob+rfq`; the order book serves the rest.

## 6. Target-User Roles & Use Cases

| Persona | What they do with Exchange8:RFQ |
|---|---|
| FX desk client (institutional) | Requests quotes on a pair/size, compares LP responses, hits the best price, deals at the shown level or lets the RFQ expire |
| Liquidity provider (sales/FX trader) | Answers RFQs inside the window, exercises bounded last look on hits, serves streaming two-way prices |
| Venue operations | Monitors RFQ flow, LP session health, expired/missed windows, last-look incidents |
| Market-data consumer | Watches the anonymized tape for RFQ liquidity signals without identity |
| Risk manager | Reviews per-currency net exposure and limits produced by FX dealing |
| Exchange admin | Manages the pair and tenor reference data, administers LP eligibility, authors Shield policy for the service via the admin surface |
| Compliance / surveillance | Reads last-look reject records, late-answer counters and the tape for market-conduct investigation |

## 7. KPIs / Success Criteria

- **Fairness**: 100% of RFQs fan out to all entitled LPs with no
  differential reveal time (by construction; asserted by conformance
  scenarios).
- **Late-quote containment**: zero late quotes presented to clients or
  bound in a trade since this behaviour is structurally enforced.
- **Price certainty**: accepted and past-deadline last-look accepts fill at
  the quoted price - zero trades printed off-quote.
- **Window discipline**: RFQs expire within their window plus bound grace
  (sweep); expired RFQs produce exactly one `rfq:expired` event each.
- **LP accountability**: every last-look rejection (with reason) and every
  late last-look answer is captured in the scorecard with a derivable
  reject rate.
- **Tape integrity**: zero identity-bearing fields on the public tape;
  one-sided and identity-bearing events structurally excluded.
- **Reliability**: no RFQ state change lost - outbox + journal ride the
  same transaction; Bus/Sys outages degrade delivery, never lose records.
- **Latency**: quote ingest and hit decisions are in-memory engine
  operations (sub-millisecond core path); persistence is per transition.

## 8. Dependencies & Reliance

| Relies on | For |
|---|---|
| Exchange8:Gateway | Client and LP FIX 4.4 / WebSocket session transports and authentication of connections |
| Exchange8:RefData | Security master and currency catalog (security → FX pair extension; currencies) |
| Exchange8:Risk | Currency/tenor limit policy and the 21-check risk matrix applied to RFQ-initiated flows |
| Exchange8:ME | The CLOB serving model for pairs traded on the order book |
| Exchange8:Clearing / Settlement / Bank8:Ledger | Position, novation and cash settlement of RFQ trades |
| Exchange8:MD / Exchange8:VFeeds | Consumer-facing market-data distribution of tape/stream content |
| Sentin8:Auth | Identity, user and M2M tokens (cert-bound, DCR-registered per workload) |
| Sentin8:Bus | Durable delivery of RFQ lifecycle events (outbox → produce) |
| MemBus (Valkey) | Newest-wins streaming quote and tape publication |
| Sentin8:Sys | Audit ingest (`POST /v1/audit`) and host-context resolution |
| Shield (sentin8-policy) | The authorization decision on every handler surface |
| Sentin8:Proxy / `sentin8-tls` | Single ingress, in-process post-quantum TLS |
| PostgreSQL 18 (db `exchange8`, schema `rfq`) | Durable state under RLS |

## 9. Acceptance Criteria

1. A client RFQ reaches every LP entitled for the pair at the same moment;
   an RFQ with no entitled LP terminates immediately as expired.
2. An LP response inside the window is presented to the client and moves the
   RFQ to quoted; a response outside the window is recorded and invisible.
3. The client's hit binds only at the exactly-quoted deal leg; any other
   price level is refused without state change.
4. A hit past the quote's validity expires the RFQ; the client receives the
   expired outcome.
5. On LP last look: an accept prints the trade at the quoted price even if
   the answer is late; a reject declines the deal, publishes the reason,
   persists the rejection and counts it against the LP.
6. An LP that never answers last look is swept: the trade fills at the
   quoted price and the lateness is counted.
7. A streaming hit binds the LP's newest valid, non-withdrawn quote;
   withdrawal and expiry take effect for subsequent hits immediately.
8. Streaming deals obtain trade ids from the same sequence as RFQ deals and
   are printed with no RFQ reference.
9. The tape contains only pair, two-way price, size and deal prints - a
   serialized tape entry is provably free of LP and client identity.
10. Every RFQ transition is persisted with its outbox and audit rows in one
    transaction and survives a relay or audit-outage retry losslessly.
11. All tenant tables enforce environment isolation end-to-end: no context,
    no rows; foreign context, no visible rows.
12. Admin surfaces expose RFQ monitoring, pair/tenor reference data and LP
    session state under the declared admin scopes, policy under
    `exchange8:rfq:policy:read|write`.

## 10. Open Business Questions

- Entitlement revocation economics: the reject-rate gate exists as data;
  the business process (thresholds, warning cadence, entitlement suspension)
  that consumes the scorecards is a venue-policy decision.
- RFQ pricing beyond spot (forward tenor curves / forward-point sources) is
  not yet a business decision on this surface; today the catalog carries
  tenor rules, and forward-priced quotes are served by the LPs' own marks.
- The split between public tape and commercial "RFQ depth" products
  (retters of quote activity to entitled subscribers) has tape primitives
  only; any paid distribution tier is an MD/VFeeds product decision.
- Streaming-quote publication scope (which entitled sessions see an LP's
  stream beyond its own clients) is undefined; today it is session-private.
- Whether dealt RFQ trades should be disclosed on the public tape
  immediately or delayed (pre-trade transparency vs post-trade publish) is
  a market-conduct policy question, not yet settled.
- Sweep/cut-off jobs (pair trading cutoffs, day-end exposure rollover) are
  data fields today; their scheduling lives outside this service until the
  Cron-schedule wiring is settled.

---

**Design attribution:** exchange8:rfq and its underlying concepts were designed by Hamid Jahanzad <Hamid@Jahanzad.com>.
