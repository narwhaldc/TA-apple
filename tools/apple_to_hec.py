#!/usr/bin/env python3
"""
apple_to_hec.py — TA-apple ingest (Health Auto Export -> synced folder -> Splunk HEC).

Apple Health / HealthKit is on-device only (no cloud API). The iOS app "Health
Auto Export" (HAE) writes JSON export files to a folder your sync client keeps
current (iCloud Drive on a Mac, Dropbox on Linux, an NFS/SMB mount, etc.). This
script PULLS those files from a plain directory, explodes them into clean
per-record CANONICAL events, and delivers them to HEC (index=wearables).

Design notes (see INSTALL.md):
  * Folder-agnostic + pure-Python (json + requests) -> runs on Linux or macOS.
    It does NOT talk to iCloud/Dropbox; the sync client owns that auth. The only
    secret this script needs is the HEC token (apple_targets.json / .env).
  * Apple Health is an AGGREGATOR: each sample carries its origin `source`
    (e.g. "Oura", "Narwhal Ultra 2", "Narwhal Ultra 2|Oura"). We rename that to
    `hk_source` (`source` is a RESERVED Splunk field) and derive canonical
    `vendor` from it. `skip_sources` (default empty) lets you drop a vendor you
    also pull with its own TA, to avoid double-counting.
  * Units are locale-dependent (kg/lb, km/mi) -> converted to SI (kg, meters).
  * Apple calls light sleep `core` -> mapped to light_min.
  * Per-minute heart_rate firehose is OFF by default (we already get
    resting_heart_rate daily); enable with hr_firehose.
  * File lifecycle: skip in-flight/placeholder files -> parse -> explode ->
    dedup -> POST -> delete (or archive). Nothing is deleted until every POST
    for the run returns 200, so a failed run just retries next time.

Config: apple_targets.json (preferred) or env SPLUNK_HEC_URL/TOKEN + APPLE_*.
CLI: --status  --dry-run  --keep(archive/keep files)  --file PATH  --target NAME
     --person PID
"""

import argparse, atexit, collections, datetime, fcntl, glob, hashlib, json, os, sys, time
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("missing dependency: pip install requests")

HERE = Path(__file__).resolve().parent


# ---- Splunk-friendly logging (logfmt: <ts> level=.. comp=apple msg=".." key=val) ----
_LOG_COMPONENT = "apple"


def _logfmt(v):
    s = str(v)
    return '"' + s.replace('"', "'") + '"' if (s == "" or " " in s or "=" in s) else s


def _log(level, msg, **kv):
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    extra = "".join(" %s=%s" % (k, _logfmt(v)) for k, v in kv.items())
    print("%s level=%s comp=%s msg=%s%s" % (ts, level, _LOG_COMPONENT, _logfmt(msg), extra),
          file=sys.stderr)


def log_info(msg, **kv):  _log("INFO", msg, **kv)
def log_warn(msg, **kv):  _log("WARN", msg, **kv)
def log_error(msg, **kv): _log("ERROR", msg, **kv)


# ------------------------------------------------------------------ .env autoload
def load_dotenv():
    """Populate os.environ from a local .env (KEY=VALUE) next to this script.
    Existing env wins; a leading 'export ' and surrounding quotes are stripped.
    .env is gitignored (it may hold the HEC token) — never commit it."""
    path = HERE / ".env"
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[len("export "):]
                if "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key, val = key.strip(), val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except IOError:
        pass


load_dotenv()

TARGETS_FILE = Path(os.getenv("APPLE_TARGETS_FILE", HERE / "apple_targets.json"))
DEDUP_FILE   = Path(os.getenv("APPLE_DEDUP_FILE",   HERE / "apple_dedup_store.json"))
LOCK_FILE    = Path(os.getenv("APPLE_LOCK_FILE",    HERE / "apple_sync.lock"))
DEDUP_MAX    = int(os.getenv("APPLE_DEDUP_MAX", "50000"))


# ------------------------------------------------------------------ config (source + targets)
def load_config(target_filter=None):
    """Returns (source_cfg, targets). The file SOURCE (watch_dir + archive/lifecycle)
    is ONE top-level 'source' block shared by all targets: each file is read once,
    fanned out to EVERY target, and cleaned up ONCE (only after all targets succeed).
    Targets are pure destinations (hec_*, index, person_id, verify_ssl, skip_sources,
    hr_firehose). Back-compat: if there is no top-level 'source', it is derived from
    the first target that still carries watch_dir (with a deprecation note)."""
    if TARGETS_FILE.exists():
        try:
            raw = json.loads(TARGETS_FILE.read_text())
        except Exception as e:
            sys.exit(f"failed to read {TARGETS_FILE}: {e}")
        raw_targets = raw.get("targets", {})
        targets = {}
        for name, cfg in raw_targets.items():
            if name.startswith("_"):
                continue
            if not cfg.get("hec_url") or not cfg.get("hec_token"):
                log_warn("target missing hec_url/hec_token; skipping", target=name); continue
            if not cfg.get("person_id"):
                log_warn("target missing person_id (required for RBAC)", target=name)
            targets[name] = _norm_target(cfg)
        src_raw = raw.get("source")
        if src_raw is None:
            for name, cfg in raw_targets.items():
                if cfg.get("watch_dir"):
                    log_warn("no top-level 'source' block; using target watch_dir (move to a "
                             "shared 'source' block so every target gets the data)", target=name)
                    src_raw = cfg; break
        source = _norm_source(src_raw or {})
    else:
        url, tok = os.getenv("SPLUNK_HEC_URL"), os.getenv("SPLUNK_HEC_TOKEN")
        if not (url and tok):
            sys.exit(f"no config: create {TARGETS_FILE} (see apple_targets.example.json) "
                     f"or set SPLUNK_HEC_URL + SPLUNK_HEC_TOKEN + APPLE_WATCH_DIR")
        targets = {"default": _norm_target({
            "hec_url": url, "hec_token": tok,
            "index": os.getenv("WEARABLES_INDEX", "wearables"),
            "person_id": os.getenv("APPLE_PERSON_ID", "P001"),
            "verify_ssl": os.getenv("SPLUNK_HEC_VERIFY", "1") != "0"})}
        source = _norm_source({"watch_dir": os.getenv("APPLE_WATCH_DIR", ""),
                               "archive_dir": os.getenv("APPLE_ARCHIVE_DIR")})
    if not targets:
        sys.exit(f"no targets in {TARGETS_FILE}")
    if not source["watch_dir"]:
        sys.exit("no watch_dir: add a top-level 'source' block with watch_dir "
                 "(see apple_targets.example.json)")
    if target_filter:
        if target_filter not in targets:
            sys.exit(f"target '{target_filter}' not found. have: {list(targets)}")
        targets = {target_filter: targets[target_filter]}
    return source, targets


def _norm_target(cfg):
    """A DESTINATION: where to send + how to attribute/filter. No file lifecycle here."""
    return {
        "hec_url": cfg["hec_url"], "hec_token": cfg["hec_token"],
        "index": cfg.get("index", "wearables"),
        "person_id": cfg.get("person_id"),
        "verify_ssl": cfg.get("verify_ssl", True),
        "skip_sources": [s.lower() for s in cfg.get("skip_sources", [])],
        "hr_firehose": bool(cfg.get("hr_firehose", False)),
    }


def _norm_source(cfg):
    """The shared file folder + lifecycle, common to all targets."""
    ad = cfg.get("archive_dir")
    return {
        "watch_dir": os.path.expanduser(cfg.get("watch_dir", "") or ""),
        "file_glob": cfg.get("file_glob", "*.json"),
        "delete_after_ingest": bool(cfg.get("delete_after_ingest", True)),
        "archive_dir": os.path.expanduser(ad) if ad else None,
        "min_file_age_seconds": int(cfg.get("min_file_age_seconds", 60)),
    }


# ------------------------------------------------------------------ state / dedup
def load_json(path, default):
    if path.exists():
        try: return json.loads(path.read_text())
        except Exception as e: log_warn("could not read state file; starting fresh", path=str(path), error=type(e).__name__)
    return default

def save_json(path, obj):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, separators=(",", ":"), default=str))
    tmp.replace(path)

def _hash(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


# ------------------------------------------------------------------ mapping helpers
VENDOR_KEYWORDS = [("oura", "oura"), ("garmin", "garmin"), ("withings", "withings"),
                   ("fitbit", "fitbit"), ("whoop", "whoop"), ("polar", "polar"),
                   ("wahoo", "wahoo"), ("peloton", "peloton"), ("hume", "hume")]

def source_to_vendor(hk_source):
    """Map an Apple HealthKit source string to a canonical vendor. A pipe-delimited
    multi-source (e.g. 'Narwhal Ultra 2|Oura') is Apple's merged view -> apple."""
    raw = hk_source or ""
    if "|" in raw:
        return "apple"
    s = raw.lower()
    for kw, v in VENDOR_KEYWORDS:
        if kw in s:
            return v
    return "apple"

def to_kg(qty, units):
    u = (units or "").lower()
    if u in ("lb", "lbs", "pound", "pounds"): return qty * 0.45359237
    if u in ("g", "gram", "grams"):           return qty / 1000.0
    if u in ("st", "stone"):                  return qty * 6.35029318
    return qty  # already kg

def to_meters(qty, units):
    u = (units or "").lower()
    if u in ("mi", "mile", "miles"): return qty * 1609.344
    if u in ("km",):                 return qty * 1000.0
    if u in ("ft", "feet"):          return qty * 0.3048
    if u in ("yd", "yard", "yards"): return qty * 0.9144
    return qty  # already m

def as_pct(qty):
    """Apple can express % as a fraction (0.97) or a number (97). Normalize to 0-100."""
    return qty * 100.0 if (qty is not None and qty <= 1.0) else qty

def to_celsius(qty, units):
    u = (units or "").lower().replace("°", "")
    if u in ("degf", "f", "fahrenheit"): return (qty - 32.0) * 5.0 / 9.0
    if u in ("degk", "k", "kelvin"):     return qty - 273.15
    return qty  # already C

def sample_value(sample):
    """Most samples use 'qty'; HR-style samples use Avg/Min/Max."""
    if sample.get("qty") is not None:
        return sample["qty"]
    return sample.get("Avg")

def parse_dt(s):
    """'2026-07-29 08:00:00 -0400' -> (epoch, 'YYYY-MM-DD')."""
    if not s:
        return None, None
    try:
        dt = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S %z")
    except ValueError:
        try:
            dt = datetime.datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None, None
    return dt.timestamp(), dt.strftime("%Y-%m-%d")


# name -> (sourcetype, canonical_field, kind)
# kinds: sum_daily, sum_daily_dist, avg_daily, avg_daily_pct,
#        bodycomp, bodycomp_mass, bodycomp_pct, hr_firehose
METRIC_MAP = {
    "step_count":                 ("apple:activity", "steps",            "sum_daily"),
    "walking_running_distance":   ("apple:activity", "distance_m",       "sum_daily_dist"),
    "active_energy":              ("apple:activity", "active_calories",  "sum_daily"),
    "basal_energy_burned":        ("apple:activity", "basal_calories",   "sum_daily"),
    "apple_exercise_time":        ("apple:activity", "active_min",       "sum_daily"),
    "flights_climbed":            ("apple:activity", "floors",           "sum_daily"),
    "resting_heart_rate":         ("apple:daily",    "resting_hr",       "avg_daily"),
    "heart_rate_variability":     ("apple:daily",    "hrv_avg",          "avg_daily"),
    "respiratory_rate":           ("apple:daily",    "respiration_avg",  "avg_daily"),
    "blood_oxygen_saturation":    ("apple:daily",    "spo2_avg",         "avg_daily_pct"),
    "apple_sleeping_wrist_temperature": ("apple:daily", "skin_temp",     "avg_daily_temp"),
    "walking_heart_rate_average": ("apple:daily",    "walking_hr_avg",   "avg_daily"),
    "vo2_max":                    ("apple:daily",    "vo2max",           "avg_daily"),
    "weight_body_mass":           ("apple:bodycomp", "weight_kg",        "bodycomp_mass"),
    "body_mass_index":            ("apple:bodycomp", "bmi",              "bodycomp"),
    "body_fat_percentage":        ("apple:bodycomp", "body_fat_pct",     "bodycomp_pct"),
    "lean_body_mass":             ("apple:bodycomp", "lean_mass_kg",     "bodycomp_mass"),
    "heart_rate":                 ("apple:heartrate","bpm",              "hr_firehose"),
}


# ------------------------------------------------------------------ explode one payload
def explode(payload, tgt):
    """Turn one HAE JSON payload into a list of (sourcetype, epoch, event_dict).
    Aggregates day+vendor totals/averages; emits body-comp & sleep per record."""
    metrics = (payload.get("data") or {}).get("metrics") or []
    # accumulators keyed by (day, vendor)
    act = collections.defaultdict(dict)                 # sum fields
    dayvit = collections.defaultdict(lambda: collections.defaultdict(list))  # values to avg
    events = []
    unmapped = set()
    extra = collections.defaultdict(list)   # (metric, day, vendor) -> [values]; unmapped -> apple:extra
    extra_units = {}

    for metric in metrics:
        name = metric.get("name")
        units = metric.get("units")
        if name == "sleep_analysis":
            events += _sleep_events(metric, tgt)
            continue
        route = METRIC_MAP.get(name)
        if not route:
            # no canonical mapping -> keep it anyway in the generic apple:extra bucket
            unmapped.add(name)
            for s in metric.get("data") or []:
                vendor = source_to_vendor(s.get("source"))
                if vendor in tgt["skip_sources"]:
                    continue
                _, day = parse_dt(s.get("date"))
                v = sample_value(s)
                if day is None or not isinstance(v, (int, float)):
                    continue
                extra[(name, day, vendor)].append(v)
                extra_units[name] = units
            continue
        st, field, kind = route
        for s in metric.get("data") or []:
            vendor = source_to_vendor(s.get("source"))
            if vendor in tgt["skip_sources"]:
                continue
            epoch, day = parse_dt(s.get("date"))
            if day is None:
                continue
            v = sample_value(s)
            if v is None:
                continue
            key = (day, vendor)
            if kind == "sum_daily":
                act[key][field] = act[key].get(field, 0) + v
            elif kind == "sum_daily_dist":
                act[key][field] = act[key].get(field, 0) + to_meters(v, units)
            elif kind == "avg_daily":
                dayvit[key][field].append(v)
            elif kind == "avg_daily_pct":
                dayvit[key][field].append(as_pct(v))
            elif kind == "avg_daily_temp":
                dayvit[key][field].append(to_celsius(v, units))
            elif kind.startswith("bodycomp"):
                val = to_kg(v, units) if kind == "bodycomp_mass" else (as_pct(v) if kind == "bodycomp_pct" else v)
                events.append((st, epoch, {field: round(val, 2), "day": day,
                                           "hk_source": s.get("source", ""), "_vendor": vendor}))
            elif kind == "hr_firehose":
                if tgt["hr_firehose"]:
                    events.append((st, epoch, {"bpm": v, "day": day,
                                               "hk_source": s.get("source", ""), "_vendor": vendor}))

    # flush activity sums (one apple:activity event per day+vendor)
    for (day, vendor), fields in act.items():
        ev = {k: (round(x) if k in ("steps", "floors") else round(x, 2)) for k, x in fields.items()}
        ev.update({"day": day, "_vendor": vendor})
        events.append(("apple:activity", _midnight(day), ev))
    # flush daily vitals averages (one apple:daily event per day+vendor)
    for (day, vendor), fields in dayvit.items():
        ev = {k: round(sum(vs) / len(vs), 2) for k, vs in fields.items() if vs}
        ev.update({"day": day, "_vendor": vendor})
        events.append(("apple:daily", _midnight(day), ev))

    # flush unmapped metrics into the generic apple:extra bucket (one row per metric/day/vendor,
    # multi-stat so the consumer picks: avg for rates, sum for totals, count for events).
    for (metric, day, vendor), vals in extra.items():
        if not vals:
            continue
        events.append(("apple:extra", _midnight(day), {
            "metric": metric, "units": extra_units.get(metric),
            "count": len(vals), "sum": round(sum(vals), 3),
            "avg": round(sum(vals) / len(vals), 3), "min": min(vals), "max": max(vals),
            "day": day, "_vendor": vendor}))

    events += _workout_events((payload.get("data") or {}).get("workouts") or [], tgt)
    if unmapped:
        log_info("unmapped metrics routed to apple:extra", count=len(unmapped),
                 metrics=",".join(sorted(unmapped)))
    return events


def _sleep_events(metric, tgt):
    """sleep_analysis -> one apple:sleep event per night. Apple 'core' = light."""
    out = []
    for s in metric.get("data") or []:
        vendor = source_to_vendor(s.get("source"))
        if vendor in tgt["skip_sources"]:
            continue
        start_epoch, _ = parse_dt(s.get("sleepStart") or s.get("inBedStart"))
        _, day = parse_dt(s.get("date") or s.get("sleepStart"))
        h = lambda k: round((s.get(k) or 0) * 60.0, 1)   # hours -> minutes
        tib = None
        ib0, _ = parse_dt(s.get("inBedStart"))
        ib1, _ = parse_dt(s.get("inBedEnd"))
        if ib0 and ib1 and ib1 > ib0:
            tib = round((ib1 - ib0) / 60.0, 1)
        total = h("totalSleep")
        ev = {"total_sleep_min": total, "deep_min": h("deep"), "rem_min": h("rem"),
              "light_min": h("core"), "awake_min": h("awake"), "day": day,
              "sleep_type": "long_sleep", "hk_source": s.get("source", ""), "_vendor": vendor}
        if tib:
            ev["time_in_bed_min"] = tib
            ev["efficiency_pct"] = round(100.0 * total / tib, 1) if tib else None
        out.append(("apple:sleep", start_epoch or _midnight(day), ev))
    return out


# Workout sources that ECHO a workout (a direct-vendor pull or a sync/aggregator app)
# rather than the device that actually recorded it. Used to prefer the real recorder
# when the same physical workout appears multiple times.
_ECHO_SOURCES = {"oura", "garmin", "withings", "fitbit", "whoop", "polar",
                 "rungap", "healthfit", "strava", "runkeeper"}

def _has_device_source(hk_source):
    """True if any pipe-token is a real recording device (not a known vendor/sync app)."""
    toks = [t.strip().lower() for t in (hk_source or "").split("|") if t.strip()]
    return any(t not in _ECHO_SOURCES for t in toks)

def _dedup_workouts(cands):
    """Apple Health echoes one physical workout across sources (Apple Watch, Oura,
    RunGap, ...). Cluster same-activity workouts with overlapping time windows and keep
    ONE — prefer a real recording device, then longest duration, then most calories."""
    clusters = []
    for c in sorted(cands, key=lambda x: (x["activity"], x["start"] or 0)):
        s, e = c["start"], (c["end"] or c["start"])
        placed = False
        for cl in clusters:
            if cl["activity"] == c["activity"] and s is not None and \
               s < cl["end"] and e > cl["start"]:            # time intervals overlap
                cl["items"].append(c)
                cl["start"] = min(cl["start"], s); cl["end"] = max(cl["end"], e)
                placed = True; break
        if not placed:
            clusters.append({"activity": c["activity"], "start": s, "end": e, "items": [c]})
    _score = lambda c: (1 if _has_device_source(c["hk_source"]) else 0, c["dur"] or 0, c["cals"] or 0)
    return [max(cl["items"], key=_score) for cl in clusters]

def _workout_events(workouts, tgt):
    """data.workouts[] -> apple:workout events, de-duplicated across recording sources.
    HAE workout objects have no top-level `source` (derive vendor from nested samples);
    `distance` is present only for distance sports (run/walk/ride)."""
    cands = []
    for w in workouts:
        wsrc = ""
        for arr in ("heartRateData", "activeEnergy", "stepCount", "basalEnergy"):
            s = w.get(arr) or []
            if s and isinstance(s[0], dict) and s[0].get("source"):
                wsrc = s[0]["source"]; break
        vendor = source_to_vendor(wsrc)
        start_epoch, day = parse_dt(w.get("start"))
        end_epoch, _ = parse_dt(w.get("end"))
        def _qty(k):
            o = w.get(k) or {}
            return o.get("qty") if isinstance(o, dict) else None
        ev = {"workout_activity": w.get("name"), "workout_id": w.get("id"),
              "day": day, "hk_source": wsrc, "_vendor": vendor}
        if start_epoch: ev["workout_start_epoch"] = int(start_epoch)
        if end_epoch:   ev["workout_end_epoch"] = int(end_epoch)
        dur = w.get("duration")
        if isinstance(dur, (int, float)): ev["workout_duration_min"] = round(dur / 60.0, 1)
        if _qty("totalEnergy") is not None:        ev["workout_calories"] = round(_qty("totalEnergy"), 1)
        if _qty("activeEnergyBurned") is not None: ev["workout_active_calories"] = round(_qty("activeEnergyBurned"), 1)
        if _qty("avgHeartRate") is not None:       ev["workout_avg_hr"] = round(_qty("avgHeartRate"))
        if _qty("maxHeartRate") is not None:       ev["workout_max_hr"] = round(_qty("maxHeartRate"))
        dist = w.get("distance") or {}
        if isinstance(dist, dict) and dist.get("qty") is not None:
            ev["workout_distance_m"] = round(to_meters(dist["qty"], dist.get("units")), 1)
        cands.append({"activity": w.get("name") or "", "start": start_epoch,
                      "end": end_epoch or start_epoch,
                      "dur": (dur if isinstance(dur, (int, float)) else 0),
                      "cals": (_qty("totalEnergy") or 0), "vendor": vendor,
                      "hk_source": wsrc, "epoch": start_epoch or _midnight(day), "ev": ev})
    out = []
    for c in _dedup_workouts(cands):          # dedup FIRST, then apply skip_sources to survivors
        if c["vendor"] in tgt["skip_sources"]:
            continue
        out.append(("apple:workout", c["epoch"], c["ev"]))
    return out


def _midnight(day):
    try:
        return time.mktime(datetime.datetime.strptime(day, "%Y-%m-%d").timetuple())
    except Exception:
        return time.time()


# ------------------------------------------------------------------ HEC
def to_hec(tgt, sourcetype, epoch, ev):
    vendor = ev.pop("_vendor", "apple")
    return {"time": epoch if epoch else time.time(), "event": ev, "sourcetype": sourcetype,
            "index": tgt["index"], "source": "health_auto_export",
            "fields": {"vendor": vendor, "person_id": tgt["person_id"]}}

def hec_send(tgt, batch):
    body = "".join(json.dumps(e) for e in batch)
    verify = tgt.get("verify_ssl", True) if tgt["hec_url"].startswith("https") else False
    r = requests.post(tgt["hec_url"], data=body,
                      headers={"Authorization": f"Splunk {tgt['hec_token']}"},
                      verify=verify, timeout=120)
    if r.status_code >= 300:
        # surface HEC's reason (e.g. Incorrect index / invalid token) instead of a bare 400
        raise RuntimeError(f"HTTP {r.status_code}: {r.text.strip()[:300]}")


# ------------------------------------------------------------------ file discovery
def is_icloud_placeholder(p):
    """macOS iCloud can evict a file to a 0-byte '.name.icloud' stub. Best-effort
    trigger a download; only relevant on Darwin."""
    if sys.platform != "darwin":
        return False
    stub = p.parent / ("." + p.name + ".icloud")
    if stub.exists():
        os.system(f"brctl download {json.dumps(str(stub))} >/dev/null 2>&1")
        time.sleep(2)
        return not p.exists()
    return False

def discover_files(source):
    wd = source["watch_dir"]
    if not wd or not os.path.isdir(wd):
        sys.exit(f"watch_dir not found: {wd!r} — set it in the 'source' block of apple_targets.json")
    now = time.time()
    files = []
    for path in sorted(glob.glob(os.path.join(wd, source["file_glob"]))):
        p = Path(path)
        if is_icloud_placeholder(p):
            log_warn("skipping iCloud placeholder not yet downloaded", file=p.name); continue
        try:
            age = now - p.stat().st_mtime
        except OSError:
            continue
        if age < source["min_file_age_seconds"]:
            log_warn("skipping file still syncing", file=p.name, age_s=int(age)); continue
        files.append(p)
    return files

def finish_file(source, p, dry):
    """Called ONCE per file, after every target has ingested it."""
    if dry:
        return
    if source["archive_dir"]:
        os.makedirs(source["archive_dir"], exist_ok=True)
        p.replace(Path(source["archive_dir"]) / p.name)
    elif source["delete_after_ingest"]:
        try: p.unlink()
        except OSError as e: log_warn("could not delete processed file", file=p.name, error=type(e).__name__)


# ------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser(description="Apple Health (HAE) file-drop -> HEC")
    ap.add_argument("--target", help="only this target from apple_targets.json")
    ap.add_argument("--file", help="ingest a single specific file (skips folder discovery)")
    ap.add_argument("--person", help="override person_id for all targets (testing)")
    ap.add_argument("--dry-run", action="store_true", help="parse + report, no HEC send, no delete")
    ap.add_argument("--keep", action="store_true", help="do not delete/archive processed files")
    ap.add_argument("--status", action="store_true", help="show dedup store summary and exit")
    ap.add_argument("--reset-dedup", action="store_true", help="clear the dedup store")
    args = ap.parse_args()

    if args.reset_dedup and DEDUP_FILE.exists():
        DEDUP_FILE.unlink(); print(f"dedup store cleared ({DEDUP_FILE})")

    source, targets = load_config(args.target)
    if args.person:
        for t in targets.values():
            t["person_id"] = args.person
        print(f"[override] person_id -> {args.person} for all targets")

    store = load_json(DEDUP_FILE, {})
    if args.status:
        print(f"watch_dir: {source['watch_dir']}   targets: {', '.join(targets)}")
        for n in targets:
            print(f"  {n}: {len(store.get(n, {}))} deduped records")
        return

    # single-instance lock (cron + manual can't corrupt the dedup store)
    lock_fp = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log_warn("another run holds the lock; exiting", lock_file=str(LOCK_FILE))
        sys.exit(1)
    def _release_lock():
        # Unlock + close + remove the lock FILE on clean exit. Registered only after
        # we hold the flock, so a losing instance can't delete another's lock. On a
        # hard crash the file may linger, but flock auto-releases on process death so
        # the next run just re-acquires it.
        try: fcntl.flock(lock_fp, fcntl.LOCK_UN)
        except Exception: pass
        try: lock_fp.close()
        except Exception: pass
        try: os.unlink(LOCK_FILE)
        except OSError: pass
    atexit.register(_release_lock)

    files = [Path(args.file)] if args.file else discover_files(source)
    t0 = time.time()
    log_info("run started", watch_dir=source["watch_dir"], targets=len(targets),
             files=len(files), dry_run=args.dry_run)
    if not files:
        log_info("run complete", events=0, skipped=0, files=0, failures=0, targets=len(targets),
                 duration_s=round(time.time() - t0, 1), dry_run=args.dry_run)
        return

    total_sent = total_skip = failed = 0
    try:
        for p in files:
            try:
                payload = json.loads(p.read_text())
            except Exception as e:
                failed += 1
                log_error("unparseable file; will retry next run", file=p.name, error=type(e).__name__); continue
            # Read once, fan out to EVERY target; finish (archive/delete) only if all succeed.
            file_ok = True
            for tname, tgt in targets.items():
                tstore = store.setdefault(tname, {})
                events = explode(payload, tgt)            # per-target: skip_sources/hr_firehose differ
                batch = []
                for st, epoch, ev in events:
                    h = _hash({"st": st, "p": tgt["person_id"], "ev": ev, "t": int(epoch or 0)})
                    if h in tstore:
                        total_skip += 1; continue
                    batch.append((h, to_hec(tgt, st, epoch, dict(ev))))
                sent_here = 0
                for i in range(0, len(batch), 200):
                    chunk = batch[i:i+200]
                    if args.dry_run:
                        continue
                    try:
                        hec_send(tgt, [e for _, e in chunk])
                    except Exception as e:
                        failed += 1
                        log_error("HEC send failed; will retry next run", file=p.name, target=tname,
                                  error=type(e).__name__, detail=str(e)); file_ok = False; break
                    for h, _ in chunk:
                        tstore[h] = 1
                    sent_here += len(chunk); total_sent += len(chunk)
                log_info("sent events", person_id=tgt.get("person_id"), target=tname, file=p.name,
                         events=len(events), new=len(batch), count=sent_here)
            if file_ok and not args.dry_run:
                finish_file(source, p, dry=args.keep)
                if not args.keep:
                    log_info("finished file", file=p.name,
                             action=("archived" if source["archive_dir"] else "deleted"))
    except Exception as e:
        log_error("run failed", error=type(e).__name__, detail=str(e),
                  duration_s=round(time.time() - t0, 1))
        sys.exit(1)

    # prune + persist dedup store
    for n, d in store.items():
        if len(d) > DEDUP_MAX:
            store[n] = dict(list(d.items())[-DEDUP_MAX:])
    if not args.dry_run:
        save_json(DEDUP_FILE, store)
    log_info("run complete", events=total_sent, skipped=total_skip, files=len(files),
             failures=failed, targets=len(targets), duration_s=round(time.time() - t0, 1),
             dry_run=args.dry_run)


if __name__ == "__main__":
    main()
