# TA-apple — Apple Health add-on for the Wearables platform

Normalizes **Apple Health / HealthKit** data into the canonical **Wearables** data model
(Sleep / Activity / Daily / Heart / Body-comp), so the vendor-neutral `wearables` dashboards
work with Apple data alongside Oura, Garmin, and Withings.

Apple Health has **no cloud API** (on-device only), so ingest is:

**iPhone → [Health Auto Export] (JSON files) → synced folder (iCloud/Dropbox) → `tools/apple_to_hec.py` (cron pull) → Splunk HEC (`index=wearables`)**

## Why a puller (not direct HEC)
HAE's export is one deeply-nested, per-sample JSON blob with locale units and a reserved-name
`source` field — direct-to-HEC shreds it on embedded timestamps and can't set `person_id`. The
puller parses it in Python and emits clean, per-record **canonical** events:
- explodes the metric arrays; aggregates daily totals/averages; sleep as a nightly summary
- **units → SI** (kg, meters); Apple `core` → `light_min`
- renames `source` → **`hk_source`** (reserved field) and derives canonical **`vendor`**
- stamps **`person_id`**; **dedup** store makes re-runs idempotent
- **file lifecycle:** skip in-flight/placeholder files → parse → dedup → POST → delete/archive

## Apple as a universal on-ramp
Apple Health aggregates many devices, so one flow can cover most of the model. `skip_sources`
(default empty = ingest everything) lets you drop a vendor you also pull directly, avoiding
double-counting. Apple = breadth; direct TAs = depth.

## Layout
- `default/` — props / eventtypes / tags (ships in the `.spl`)
- `tools/apple_to_hec.py` — the pull-and-forward script (**repo-only, never shipped**)
- `apple_targets.example.json` — copy to `tools/apple_targets.json` (gitignored)

Setup + security + Linux notes: see **[INSTALL.md](INSTALL.md)**.

Runs on **Linux or macOS** (pure Python: `json` + `requests`). Only secret needed is the HEC
token; PHI never enters the repo (`.gitignore` blocks all data files under `tools/`).

Apache-2.0. Part of the Wearables platform: https://github.com/narwhaldc
