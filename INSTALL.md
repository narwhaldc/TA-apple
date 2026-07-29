# TA-apple → Splunk — Installation Guide

**App version:** TA-apple 0.1.1 · Apache-2.0 · Source: https://github.com/narwhaldc

Ingest **Apple Health / HealthKit** into the canonical **Wearables** data model. HealthKit is
on-device only (no cloud API), so the iOS app **Health Auto Export (HAE)** writes JSON export
files to a synced folder, and `tools/apple_to_hec.py` pulls them to HEC.

```
iPhone (Health Auto Export, when unlocked)
   └─ export JSON files ─▶ synced folder (iCloud Drive / Dropbox / NFS / …)
                                   │
                                   ▼
        tools/apple_to_hec.py  (cron PULL — like TA-oura/TA-garmin)
          reads files → explodes arrays → units→SI → core→light →
          source→hk_source + derive vendor → dedup → stamp person_id →
          HEC /services/collector/event → delete (or archive) file
                                   │
                                   ▼
                     Splunk HEC → index=wearables
```

The Splunk-side **app** (this `.spl`: props/eventtypes/tags) normalizes the events into the
model. The **puller** in `tools/` is repo-only (never shipped in the `.spl`).

---

## 1. iOS — Health Auto Export automation
Install **Health Auto Export – JSON+CSV**. Create an **Automation**:
- **Data Type = Health Metrics** (start here — steps/HR/sleep/energy/SpO2/body-comp). Add a
  second automation for **Workouts** later if you want activity/GPS. Skip the sensitive types
  (Symptoms/ECG/State of Mind/Menstrual/Medications) unless you deliberately want them.
- **Include Route Data = OFF** (GPS is huge and irrelevant to health metrics).
- **Time Grouping = Hours** (or Days) — keeps files small.
- **Export Format = JSON**, **Date Range = Previous Day**, cadence daily/hourly.
- **Destination = a file/folder** your sync client keeps current (see step 2), **not** REST.

> **Verify the file shape once** (should match the REST shape): the JSON top level is
> `{ "data": { "metrics": [ { "name": …, "units": …, "data": [ … ] } ] } }`. If yours differs,
> tell us before running — one parser function may need a tweak.

## 2. Sync the folder to the host that runs the puller
The puller reads a **plain directory** — it never talks to iCloud/Dropbox itself, so pick
whatever keeps a folder current on your host:
- **macOS + iCloud Drive** — HAE → an iCloud folder; on the Mac it appears under
  `~/Library/Mobile Documents/…`. **Finder → right-click the folder → "Keep Downloaded"** so
  iCloud doesn't evict files to placeholders. (The puller also best-effort triggers a download
  on macOS if it sees a placeholder.)
- **Linux + Dropbox** — HAE → Dropbox; run the official Dropbox Linux client; point the puller
  at `~/Dropbox/<folder>`. Cleanest Linux path.
- **Linux + iCloud** — optional: use `rclone`/`pyicloud` (Apple ID **app-specific password**,
  not your real password) to drop files into the folder. Extra dependency; keep the app-password
  in `.env`/targets (gitignored, `chmod 0600`).

## 3. Configure the puller
```
cp apple_targets.example.json tools/apple_targets.json    # gitignored
chmod 600 tools/apple_targets.json
```
Edit it: **`hec_url`** must be the **event** endpoint `https://<host>:8088/services/collector/event`
(the puller sends proper event-wrapped JSON — not `/raw`), **`hec_token`**, **`person_id`**,
and **`watch_dir`** (the synced folder from step 2). Options: `skip_sources` (default empty =
ingest everything, "Apple-hub" mode), `hr_firehose` (default off), `delete_after_ingest` /
`archive_dir`, `min_file_age_seconds`. (Alternatively set `SPLUNK_HEC_URL`/`SPLUNK_HEC_TOKEN` +
`APPLE_WATCH_DIR`/`APPLE_PERSON_ID` in a gitignored `.env`.)

## 4. Run + schedule
```
python3 -m pip install requests
python3 tools/apple_to_hec.py --dry-run     # parse + report, no send, no delete
python3 tools/apple_to_hec.py               # real run
python3 tools/apple_to_hec.py --status      # dedup summary
```

**Ongoing (cron):** pull new files from your cloud remote, then run the puller — as a single
wrapper. Copy **`apple_sync.example.sh`** to a path outside the repo (e.g. `~/hae_sync.sh`),
edit the remote/paths, and cron it every ~15 min:
```
*/15 * * * * /home/YOU/hae_sync.sh
```
It runs `rclone move <remote> <watch_dir>` then the puller, sequentially. This is **decoupled
from any "cloud sync finished" signal** — each run ingests whatever has landed; partially-synced
days just finish on the next run. A `flock` lock prevents overlapping runs and the dedup store
(`tools/apple_dedup_store.json`) makes re-runs / overlapping exports idempotent (`--status` to
inspect). Use `rclone move` to keep the cloud folder tidy (or `copy` to retain the originals).

**Linux + rclone remote (headless):** install rclone, then `rclone config` a `drive`/`dropbox`
remote — answer **`n`** to "Use auto config?" and run the printed `rclone authorize` on a machine
with a browser, pasting the token back. Verify with `rclone lsf <remote>:<folder>`.

## 5. Install the Splunk app
Install the `TA-apple` `.spl` (Apps → Install app from file) on the search head/indexer that
owns `index=wearables`, and restart. It ships only `default/` (props/eventtypes/tags) +
metadata — **no `tools/`**.

---

## Apple is an aggregator — dedup policy
Every HealthKit sample carries its origin (`source`). The puller renames it to **`hk_source`**
(`source` is a reserved Splunk field) and derives canonical **`vendor`** (Oura→oura,
Garmin→garmin, Withings→withings, Apple Watch/blank/multi→apple). Default `skip_sources=[]`
ingests **everything** (Apple as a universal on-ramp). If you *also* run a direct TA for a
vendor (e.g. TA-oura) and don't want double-counting, add that source, e.g. `"skip_sources":
["Oura"]`.

## Security / no data leaks
- **Only secret the puller needs is the HEC token** — in `tools/apple_targets.json` or `.env`,
  both **gitignored**, `chmod 0600`, never committed. The sync client owns cloud auth.
- **PHI never enters the repo:** `.gitignore` blanket-ignores all data files under `tools/`
  (only `apple_to_hec.py` is tracked there) plus HAE exports, samples, and runtime state.
- **`tools/` is excluded from every `.spl`** (like the other TAs) → creds/PHI can't be packaged.
- Keep `watch_dir` and `archive_dir` **outside the repo**.
