# Skyway v250

Rebuilt from scratch off the v249 baseline after the $580 FlightAware bill. FlightAware is
gone entirely. This version runs on free sources only (FAA SWIM + OpenSky) with AeroDataBox as
an optional, capped, off-by-default supplement — and it refuses to boot if you haven't actually
configured it.

## What changed vs v249

**Killed:** FlightAware. The Apr 2026 v249 baseline's `server.js` had a hardcoded `FA_KEY`
fallback (`process.env.FA_KEY || '…'`) with no daily cap — that combination produced the bill.
That whole integration is deleted, not disabled.

**Credential rotation (do this):** The same Apr v249 tree also baked hardcoded fallbacks for
`OSKY_ID`, `OSKY_SECRET`, `SWIM_USER`, `SWIM_PASS`, and `SWIM_QUEUE` into source. Those values are
**not** copied into v250, but if that v249 zip ever left a private machine (Downloads share,
GitHub, email), **rotate OpenSky OAuth client secret and FAA SWIM password/queue credentials
now**. An Aug 2026 copy of v249 had already started nulling some of those fallbacks; treat the
Apr tree as the exposure. Secret strings are intentionally omitted from this README.

**New safeguards** (server.js):
- Refuses to boot without `OSKY_ID`, `OSKY_SECRET`, `SWIM_USER`, `SWIM_PASS`, `SWIM_QUEUE`,
  `DATABASE_URL` set — exits with a clear message instead of running degraded on a default.
- Proactive daily hard cutoff on OpenSky (v249 only reacted *after* a 429; this refuses locally
  the moment our own tracking says the budget's gone) plus a separate conservative daily cap on
  OpenSky's `/flights/*` schedule calls.
- AeroDataBox: off unless `ADB_ENABLED=1`, called **per-flight** only when SWIM+OpenSky leave a
  real gap (missing aircraft type/operator), hard-stops at 95% of `ADB_MONTHLY_UNIT_BUDGET`, and
  that counter is persisted in Postgres so a Render free-tier restart can't quietly reset it.
- Auto-pause: background polling (OpenSky schedule poll, AeroDataBox lookups) checks
  `IDLE_PAUSE_MINUTES` of zero connected browser tabs (tracked via WebSocket connections) before
  running. SWIM stays connected regardless — it's free and push-based, not per-call billed.
- Usage meter in the UI: OpenSky credit pill (ported from v249, it was already solid) plus a new
  AeroDataBox pill, both backed by `GET /status`.

**Shared backend, for real this time.** In v249, spot assignment lived *only* in the browser's
`localStorage` (not shared between people or devices), pax was an `<input>` with no save handler
at all (typed a number, it vanished on the next 30s refresh), and flags/tow-notes weren't
editable — VIP/MED/GPU badges were auto-computed from hardcoded lists, and tow notes were just an
auto-suggested string, never stored. None of that was a "backend" to move — it had to be built.
v250 adds a `ramp_state` table in Postgres (pax, spot, flags, tow_notes per tail number) with a
small REST API and WebSocket push, so /dispatch, /line-room, and /arrivals all see the same data
live.

**Three views, one backend:**
- `/dispatch` — full v249 feature set (map, ramp drag-and-drop, heli monitor, shift-handoff
  report, tail lookup) plus real editing: pax (inline), spot (unchanged from v249's click/drag
  flow, now also synced to the server), and a new ✎ button per row for flags + tow notes.
- `/line-room` — same board, large-font/high-contrast skin for a wall display. Read-mostly: the
  *only* write path is spot assignment, enforced both in the UI (pax/flags edit controls don't
  render) and server-side (`PATCH /api/line-room/ramp/:id` 403s on anything but `spot`).
- `/arrivals` — built fresh for mobile + a Honeywell CT47 (5.5" rugged Android, gloved touch):
  big tail-number hero per row, 60px+ row height, portrait/landscape reflow via CSS grid, tabs
  for Arrivals/Departures. Fully read-only — this page never sends anything but GET.

## Known limitation, please read before you rely on this

FlightAware's `/airports/KSFO/flights` gave a genuine **schedule** — flights hours out, before
anything happened. Free sources don't have a clean equivalent:

- **SWIM** gives real-time *event* pings (an arrival/departure just happened) with callsign,
  origin/destination, and aircraft type. Excellent for "what just landed," useless for "what's
  landing in 3 hours" — it has no opinion about a flight until it's airborne.
- **OpenSky's `/flights/arrival` and `/flights/departure`** are the closest thing to a schedule,
  but they're historical/lagged (OpenSky's own docs note reporting delay), not live.

Net effect: v250's arrival/departure boards will look thinner further out than v249's did, and
will fill in as SWIM events actually fire, rather than showing a pre-populated schedule. Turning
on AeroDataBox (`ADB_ENABLED=1`) closes most of this gap — that's specifically what it's there
for — but it's off by default per your instructions, and it's billed, capped, and monitored when
it's on.

## Also worth knowing: the "150nm bbox" instruction

You asked for OpenSky bboxes shrunk to 150nm for 1-credit calls. I left the client's existing
bbox logic (map view, ramp view, heli monitor — several different bboxes for different panels)
completely untouched, because per OpenSky's own credit table already in the code
(`calcCreditCost()` — ≤25 sq° = 4 credits, 25–100 = 3, 100–400 = 2, >400 = 1), a 150nm-radius box
works out to roughly 25–30 sq°, which lands in the **3-credit** tier, not 1. v249's own code
comment says an ~800nm bbox is what actually achieves 1 credit/call. I didn't want to "fix" this
by guessing at a number, so nothing about bbox sizing changed from v249 — flag it to me if you
want a specific bbox tightened and I'll size it against the real formula.

## Setup

1. `cp .env.example .env` and fill in: a `DATABASE_URL` (free Postgres — Neon, Supabase, or
   Render's own, see `render.yaml`), your OpenSky OAuth2 app (`OSKY_ID`/`OSKY_SECRET`), and your
   FAA SWIM SCDS enrollment (`SWIM_USER`/`SWIM_PASS`/`SWIM_QUEUE`).
2. `npm install`
3. `npm run check` — syntax-checks server.js
4. `node server.js` — it will refuse to start and tell you exactly what's missing if anything
   above wasn't set.
5. Open `/dispatch`, `/line-room`, `/arrivals`.

AeroDataBox stays off (`ADB_ENABLED=0` in `.env.example`) until you decide SWIM+OpenSky are
leaving real gaps. When you do: get a RapidAPI key, set `ADB_ENABLED=1` and `ADB_KEY`, and set
`ADB_MONTHLY_UNIT_BUDGET` to match your actual plan tier (Basic/free = 600 units/mo).

## Deploy

`render.yaml` provisions a free web service **and** a free Postgres instance and wires
`DATABASE_URL` between them automatically — push to GitHub, then Render → New → Blueprint. Set
the `sync: false` secrets (OSKY_*, SWIM_*, ADB_KEY) in the dashboard; they're never committed.
Render's free Postgres expires after 30 days — fine for testing, but point `DATABASE_URL` at
Neon or Supabase instead (both free, no expiry) for anything you're relying on.

`Dockerfile` works anywhere else that runs a container (Fly.io, Railway, etc.) — just supply the
same env vars and a Postgres connection string.

## Testing notes

Boot-tested on 2026-09-23 against a real local Postgres 17 instance on the build box:

| Test | Result |
|------|--------|
| `npm install` | ok (ws, pg, solclientjs) |
| `node --check server.js` | exit 0 |
| Boot with **no** required env vars | **exit 1** (lists OSKY_*, SWIM_*, DATABASE_URL) |
| Boot with `ADB_ENABLED=1` and no `ADB_KEY` (other vars set) | **exit 1** |
| Boot with dummy required env + real `DATABASE_URL` | listens on PORT; schema created |
| `GET /api/ramp` | HTTP 200 `[]` then populated |
| `PATCH /api/dispatch/ramp/N123AB` (pax/spot/flags/towNotes) | HTTP 200 |
| `PATCH /api/line-room/ramp/N123AB` with `pax` | **HTTP 403** (not silently dropped) |
| `PATCH /api/line-room/ramp/N123AB` with `spot` only | HTTP 200; **merges** (pax/flags preserved) |
| Idle-pause (`IDLE_PAUSE_MINUTES=0`) via WS connect/disconnect | `/status` `idle.paused` + `connectedClients` flip correctly |
| `GET /dispatch` `/line-room` `/arrivals` | HTTP 200 |

**Not tested (need live credentials / network):** real OpenSky OAuth2 token exchange, live FAA
SWIM SCDS login (dummy creds correctly got `Unauthorized` from the broker), real AeroDataBox
RapidAPI calls. Please verify those once against your accounts before putting this in front of
the ramp.

Feature parity was spot-checked against the Apr + Aug 2026 v249 baselines under
`/workspace/baselines/` (operator catalog 20/20, VIP, HEMS, SPOT_DEFS/suggestSpot, heli monitor,
shift handoff, tail lookup, Leaflet multi-tile fallback, credit meter).
