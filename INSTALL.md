# TA-apple → Splunk — Installation Guide

**App version:** TA-apple 0.1.2 · Apache-2.0 · Source: https://github.com/narwhaldc

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
Install **Health Auto Export – JSON+CSV**. **Note:** the app is free to *manually* export, but
**scheduled/automatic export (Automations) requires the paid subscription** — you need that tier for
the hands-off cron pipeline below. Create an **Automation**:
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
- **Linux + Google Drive (`rclone`) — recommended for a headless Linux host** — HAE exports into a
  Google Drive folder; `rclone` mirrors that folder to a local watch dir (e.g. `~/hae_inbox`) that
  the puller reads. No browser needed on the box (headless auth). **Step-by-step rclone setup is in
  the "rclone quick-start" box right below the list.**
- **macOS + iCloud Drive** — HAE → an iCloud folder; on the Mac it appears under
  `~/Library/Mobile Documents/…`. **Finder → right-click the folder → "Keep Downloaded"** so
  iCloud doesn't evict files to placeholders. (The puller also best-effort triggers a download
  on macOS if it sees a placeholder.)
- **Linux + Dropbox** — HAE → Dropbox; run the official Dropbox Linux client; point the puller
  at `~/Dropbox/<folder>`. Cleanest Linux path.
- **Linux + iCloud** — optional: use `rclone`/`pyicloud` (Apple ID **app-specific password**,
  not your real password) to drop files into the folder. Extra dependency; keep the app-password
  in `.env`/targets (gitignored, `chmod 0600`).

### rclone quick-start (Google Drive) — the recommended headless path
New to `rclone`? It's a CLI that mirrors a cloud folder to a local one. One-time setup on the host:

1. **Install rclone**
   - Linux: `curl https://rclone.org/install.sh | sudo bash`  (or `sudo apt install rclone` / `sudo dnf install rclone`)
   - macOS: `brew install rclone`
   - Confirm: `rclone version`
2. **Create the Google Drive remote** — run `rclone config` and answer the prompts:
   - `n` — New remote → **name** it `gdrive` (any name; it must match your move command / cron)
   - **Storage**: type `drive` (Google Drive)
   - `client_id` / `client_secret`: press **Enter** on both to use rclone's built-in keys (fine to start)
   - **scope**: `1` (Full access), or `2` (read-only) if the box only ever pulls
   - Edit advanced config: `n`
   - **Use auto config? → `n`** (say **no** on a headless box). rclone prints an
     `rclone authorize "drive" …` command — run **that command on any machine with a browser**
     (your Mac/laptop), approve Google in the browser, and **paste the returned token back** at the prompt.
   - Configure as a Shared Drive? `n` (unless the folder lives on a Team/Shared Drive) → `y` to confirm → `q` to quit
3. **Verify it can see the HAE export folder** (the Google Drive folder HAE writes into):
   ```
   rclone lsf gdrive:"Health Auto Export/HAE_to_splunk"
   ```
   It should list the `*.json` files HAE is dropping. (Adjust the path to your actual HAE destination folder.)
4. **Mirror to the local watch dir** the puller reads, then point `source.watch_dir` (step 3) at it:
   ```
   mkdir -p ~/hae_inbox
   rclone move gdrive:"Health Auto Export/HAE_to_splunk" ~/hae_inbox --include "*.json" --drive-use-trash=false
   ```
   `move` empties the cloud folder as it copies (keeps Drive tidy); use `copy` to retain the originals.
   Schedule this `rclone move` **immediately before** the puller in one cron line (see step 4, "Run + schedule").

**Onboarding a second person on the same box:** give them their **own** Google Drive folder + rclone
remote (or subfolder), their **own** local inbox (e.g. `~/hae_inbox_alex`), and a **second targets file**
(`cp apple_targets.json apple_targets_alex.json`; set `source.watch_dir` to their inbox and the target's
`person_id` to theirs — same `hec_url`/`hec_token`/`index`). Run their puller with
`APPLE_TARGETS_FILE=~/src/TA-apple/tools/apple_targets_alex.json python3 apple_to_hec.py`. Register their
`person_id` in the wearables app first (Admin → People).

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

## 3b. Optional: mirror ingest logs to Splunk (Ingest Health dashboard)
The puller always writes **logfmt** logs to **stderr** (`<ts> level=… comp=apple msg="…" …`) — add a
`>> apple_to_hec.log 2>&1` redirect in your cron wrapper (`hae_sync.sh`, step 4) to capture them. To
also **mirror those logs into Splunk** so the wearables **Ingest Health** dashboard can show real
success/failure/duration (not just "had new data"), add a top-level `logging` block to
`apple_targets.json`:
```json
"logging": { "method": "hec", "hec_logging_index": "wearables_log" }
```
- With `method: "hec"`, logs go to **each target's own HEC** (reusing that target's `hec_url` +
  `hec_token`) into `hec_logging_index`. Fan out to several Splunks → **each gets its own ingest
  logs** (run-level lines everywhere; per-target lines only to that target's Splunk).
- **Create a second index for the logs** — `wearables_log` — separate from the `wearables` data
  index. Splunk retention is **per-index**, so a separate index lets you keep logs ~30 days while
  health data stays for years.
- **The same HEC token must have write access to BOTH indexes** — the data index (`wearables`) and
  `hec_logging_index` (`wearables_log`). One token, two indexes.
- stderr stays on regardless; **remove the block to log to stderr only.** Logs arrive as sourcetype
  `wearables:ingest`. Endpoint overridable per target with `hec_logging_url` / `hec_logging_token`.
- **Per-person RBAC on the log index:** `person_id` is stamped as an **indexed field** on each per-target log line (`sent events` / `send failed`), and on run-level lines (`run started` / `run complete`) **only when the run is a single person**. So a person-scoped `srchFilter` on `wearables_log` shows a self-manager their own ingest health (including run start/stop/duration), while multi-person aggregator runs keep run-level lines admin-only (the aggregate `events=N` total is not leaked to individuals). To scope logs by person, add `wearables_log` to the wearables role's `srchFilter` (same person_id key as the data index).

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
with a browser, pasting the token back. Verify with `rclone lsf <remote>:<folder>`. (Full
Google-Drive walkthrough is in section 2.)

**Prune the archive.** If you set `source.archive_dir`, it's a *safety window* the puller never
cleans — add a small dedicated cron to age files out. Default ~365 days:
```
0 4 * * * find /home/tony/hae_archive_alex -type f -name "*.json" -mtime +365 -delete
```
`-type f` (files only), `-name "*.json"` (touch nothing else), `-mtime +365` (older than a year).
One line per person's archive, or point `find` at a shared parent that holds all of them. Leave
`archive_dir: null` if you'd rather the puller just delete after a successful ingest (no archive to prune).

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
