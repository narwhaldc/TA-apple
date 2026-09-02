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
  * PRIVACY-SENSITIVE categories (menstrual/reproductive, medications, mood,
    symptoms) are DROPPED BY DEFAULT and only ingested if a target explicitly
    lists the category in `optional_includes`. Fail-closed: a targets file with
    no `optional_includes` key ingests none of them, so existing configs and
    anything Apple adds in future are safe without being edited. Every drop is
    logged (one WARN per run) so a silent block is never invisible.
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


# ---- Splunk-friendly logging (logfmt: <ts> level=.. comp=.. msg=".." key=val) ----
# Duplicated identically across the TA-* print-based fetchers (only _LOG_COMPONENT
# differs); keep in sync. stderr is ALWAYS the source of truth. An optional HEC sink
# (logging.method="hec" in the targets file) mirrors the same lines to Splunk for
# dashboards; it is buffered and flushed at exit (via atexit, so a crash still ships
# the ERROR), and if the flush itself fails (e.g. HEC is the thing that's down) the
# lines are dumped to stderr and NEVER re-sent over HEC. Dry-run never flushes.
_LOG_COMPONENT = "apple"
# Fetcher version — BUMP on every fetcher change (repo-only, not in the .spl);
# emitted as fetcher_ver= on the post-sink "run started" line for drift tracking.
FETCHER_VERSION = "1.3.0"
# Box running this fetcher (its OWN hostname — not Splunk's HEC `host`). Sent as
# run_host= on run-started so Ingest Health shows which box/person to nudge to upgrade.
import socket
RUN_HOST = socket.gethostname()

_LOG_SINKS = []               # [{"url","token","index","verify","targets":set(),"buf":[]}]
_LOG_STATE = {"on": False, "dry": False, "target_pids": {}, "solo_pid": None}


def _logfmt(v):
    s = str(v)
    return '"' + s.replace('"', "'") + '"' if (s == "" or " " in s or "=" in s) else s


def _log(level, msg, **kv):
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    extra = "".join(" %s=%s" % (k, _logfmt(v)) for k, v in kv.items())
    line = "%s level=%s comp=%s msg=%s%s" % (ts, level, _LOG_COMPONENT, _logfmt(msg), extra)
    print(line, file=sys.stderr)
    if _LOG_STATE["on"]:
        tgt = kv.get("target")           # scoped: a targeted line goes only to that sink
        # Indexed person_id for RBAC: explicit kwarg, else the target's person_id, else the
        # run's solo person_id (None on multi-person runs -> run-level lines stay admin-only).
        pid = kv.get("person_id") or _LOG_STATE["target_pids"].get(tgt) or _LOG_STATE["solo_pid"]
        for sink in _LOG_SINKS:
            if tgt is None or tgt in sink["targets"]:
                sink["buf"].append((time.time(), line, pid))


def log_info(msg, **kv):  _log("INFO", msg, **kv)
def log_warn(msg, **kv):  _log("WARN", msg, **kv)
def log_error(msg, **kv): _log("ERROR", msg, **kv)


def configure_hec_log(global_cfg, targets, dry_run):
    """Set up optional per-target HEC log mirrors. `logging` config may live globally
    (top-level `logging` block) and/or per-target (a `logging` block inside a target;
    the target's block overrides the global). The HEC endpoint/index default to each
    target's OWN hec_url/hec_token/index, so a single global {"method":"hec"} fans logs
    to EVERY target's Splunk. Each mirror gets the run-level lines plus its own target's
    sent/error lines. stderr is unaffected (always on)."""
    _LOG_SINKS.clear()
    _LOG_STATE["dry"] = dry_run
    # person_id map for indexed RBAC on the log events: each target's pid, plus the
    # run's "solo" pid (set only when the whole run is one person -- see _log).
    _LOG_STATE["target_pids"] = {tn: tc.get("person_id") for tn, tc in (targets or {}).items()}
    _pids = sorted({p for p in _LOG_STATE["target_pids"].values() if p})
    _LOG_STATE["solo_pid"] = _pids[0] if len(_pids) == 1 else None
    by_key = {}
    for tname, tcfg in (targets or {}).items():
        merged = dict(global_cfg or {})
        merged.update(tcfg.get("logging") or {})
        method = merged.get("method")
        methods = method if isinstance(method, list) else ([method] if method else [])
        if "hec" not in [str(m).lower() for m in methods]:
            continue
        url = merged.get("hec_logging_url") or tcfg.get("hec_url")
        token = merged.get("hec_logging_token") or tcfg.get("hec_token")
        index = merged.get("hec_logging_index") or tcfg.get("index") or "wearables"
        if not (url and token):
            log_warn("hec log sink skipped: no hec_url/hec_token", target=tname)
            continue
        verify = merged.get("verify_ssl", tcfg.get("verify_ssl", True))
        sink = by_key.get((url, token, index))
        if sink is None:
            sink = {"url": url, "token": token, "index": index, "verify": verify,
                    "targets": set(), "buf": []}
            by_key[(url, token, index)] = sink
            _LOG_SINKS.append(sink)
        sink["targets"].add(tname)
    if _LOG_SINKS:
        _LOG_STATE["on"] = True
        atexit.register(flush_hec_log)
        log_info("hec log sink enabled", sinks=len(_LOG_SINKS),
                 hec_index=",".join(sorted({s["index"] for s in _LOG_SINKS})))


def flush_hec_log():
    """POST each sink's buffered log lines as raw logfmt events. Best-effort: a failure
    NEVER re-sends over HEC and NEVER fails the run — it dumps to stderr. Dry-run: skip."""
    for sink in _LOG_SINKS:
        buf = sink["buf"]
        sink["buf"] = []
        if not buf or _LOG_STATE["dry"]:
            continue
        events = []
        for t, line, pid in buf:
            ev = {"time": t, "event": line, "sourcetype": "wearables:ingest", "index": sink["index"]}
            if pid:
                ev["fields"] = {"person_id": pid}   # indexed field -> RBAC scoping on the log index
            events.append(json.dumps(ev))
        body = "".join(events)
        try:
            verify = sink["verify"] if str(sink["url"]).startswith("https") else False
            r = requests.post(sink["url"], data=body,
                              headers={"Authorization": "Splunk " + sink["token"]},
                              verify=verify, timeout=30)
            r.raise_for_status()
        except Exception as e:
            ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            print('%s level=WARN comp=%s msg="hec log flush failed" error=%s target=%s count=%d'
                  % (ts, _LOG_COMPONENT, type(e).__name__,
                     ",".join(sorted(sink["targets"])), len(buf)), file=sys.stderr)


def load_logging_cfg():
    """Top-level `logging` block from the targets file (or {} if none/absent)."""
    try:
        return (json.loads(open(str(TARGETS_FILE)).read()) or {}).get("logging") or {}
    except Exception:
        return {}


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
        # FAIL-CLOSED: a missing key means an EMPTY set (ingest none of the sensitive
        # categories) — never "ingest everything". That is what makes every pre-existing
        # targets file safe with no edit. Lowercased so "womanHealth"/"womanhealth" both work.
        "optional_includes": {str(c).strip().lower() for c in cfg.get("optional_includes", [])},
        "hr_firehose": bool(cfg.get("hr_firehose", False)),
        "logging": cfg.get("logging"),
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

# Genuine Apple-native sources (Apple Watch / iPhone / built-in apps) legitimately map to
# vendor=apple. Anything NOT a known third-party (VENDOR_KEYWORDS) and NOT apple-native is
# an UNKNOWN relayed source — a device that pushes into Apple Health without its own API/TA
# (RingConn, etc.). We stamp a PROVISIONAL per-source vendor (the honest ORIGIN — "apple"
# is only the transport, = sourcetype apple:*) and WARN once per run so it can be promoted
# into VENDOR_KEYWORDS. See aggregator-open-vendor-set.
# "narwhal" = this deployment's renamed Apple devices (e.g. "Narwhal Ultra 2" = Apple
# Watch Ultra 2, "Narwhal16Pro" = iPhone 16 Pro) — a renamed native device the display
# name can't otherwise reveal. Promoted here after the discovery panel surfaced them.
APPLE_NATIVE_KW = ["apple watch", "iphone", "ipad", "apple", "narwhal"]
APPLE_NATIVE_EXACT = {"health", "clock", "fitness", "siri", "workout", ""}
_UNKNOWN_SOURCES = collections.Counter()   # raw source label -> points seen this run

def _provisional_vendor(raw):
    """First alphanumeric token of the raw source, lowercased ('RingConn Health' -> 'ringconn')."""
    toks = "".join(c if c.isalnum() else " " for c in (raw or "").lower()).split()
    return toks[0] if toks else "unknown"

def source_to_vendor(hk_source):
    """Map an Apple HealthKit source string to a canonical ORIGIN vendor. A pipe-delimited
    multi-source (e.g. 'Narwhal Ultra 2|Oura') is Apple's merged view -> apple. Unknown
    third-party sources get a provisional per-source vendor + a run-level WARN (not 'apple')."""
    raw = hk_source or ""
    if "|" in raw:
        return "apple"
    s = raw.lower()
    for kw, v in VENDOR_KEYWORDS:
        if kw in s:
            return v
    for kw in APPLE_NATIVE_KW:
        if kw in s:
            return "apple"
    if s.strip() in APPLE_NATIVE_EXACT:
        return "apple"
    _UNKNOWN_SOURCES[raw] += 1
    return _provisional_vendor(raw)

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


# ------------------------------------------------------------------ sensitive-category gate
# Privacy-sensitive metrics are dropped BEFORE mapping unless the target opts in via
# `optional_includes`. This matters because unmapped metrics are otherwise ABSORBED into
# apple:extra rather than ignored — so a new HealthKit identifier, or a Health Auto Export
# automation someone enables on their phone, could start landing sensitive data in the index
# with nobody in this code having decided that.
#
# SUBSTRING match on the lowercased name, deliberately, not an exact-name allowlist: HAE's
# naming varies and Apple keeps adding identifiers (iOS 27 added menopausalState /
# bleedingAfterMenopause). A name we've never seen should be BLOCKED by default, not absorbed,
# so over-blocking is the intended bias — and every block is logged by name so an accidental
# over-block is visible and can be opted back in.
#
# Scope note: HAE exports Medications / Symptoms / State of Mind / ECG / Menstrual as
# SEPARATE automations, each its own top-level container outside `data.metrics`. This
# puller parses `data.metrics` + `data.workouts` + `data.medications` (see
# _medication_events, gated on "medicines" as a WHOLE container, not per-name-match).
# Symptoms/State of Mind/ECG/Menstrual are still NOT parsed at all -- IF a future change
# starts reading another container, it MUST be gated here too, the same way.
SENSITIVE_CATEGORIES = {
    "womanhealth": ("menstrual", "intermenstrual", "ovulation", "cervical", "contraceptive",
                    "pregnan", "menopaus", "basal_body_temperature", "sexual_activity"),
    "medicines":   ("medication", "prescription", "dose_event"),
    "mentalhealth": ("state_of_mind", "mood"),
    "symptoms":    ("symptom",),
}


def sensitive_category(name):
    """Category this metric falls under, or None. Case/separator tolerant."""
    n = (name or "").lower()
    for cat, pats in SENSITIVE_CATEGORIES.items():
        if any(p in n for p in pats):
            return cat
    return None


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
    blocked = set()                         # (metric, category) dropped by the sensitive gate
    extra = collections.defaultdict(list)   # (metric, day, vendor) -> [values]; unmapped -> apple:extra
    extra_units = {}

    for metric in metrics:
        name = metric.get("name")
        units = metric.get("units")
        # Sensitive-category gate FIRST — before sleep handling, before METRIC_MAP, and
        # before the apple:extra catch-all, so a sensitive metric is dropped whether or
        # not we have a mapping for it.
        cat = sensitive_category(name)
        if cat and cat not in tgt["optional_includes"]:
            blocked.add((name, cat))
            continue
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
    events += _medication_events((payload.get("data") or {}).get("medications") or [], tgt)
    if unmapped:
        log_info("unmapped metrics routed to apple:extra", count=len(unmapped),
                 metrics=",".join(sorted(unmapped)))
    if blocked:
        # WARN, not INFO: a block is a deliberate privacy decision the operator should SEE.
        # Names the categories so it is obvious what to add to optional_includes to allow it.
        log_warn("sensitive metrics dropped (not in optional_includes)",
                 count=len(blocked),
                 metrics=",".join(sorted(n for n, _ in blocked)),
                 categories=",".join(sorted({c for _, c in blocked})))
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
def _medication_events(meds, tgt):
    """HAE 'Medications' automation -> one apple:medications event per record.

    This is a SEPARATE top-level container (payload.data.medications), not part
    of data.metrics, so it's gated as a WHOLE (not per-record name-matching like
    SENSITIVE_CATEGORIES does for named metrics) -- every record here is
    inherently the "medicines" category. Reuses the same optional_includes gate
    and warning-log convention as the metrics-based sensitive categories.
    """
    out = []
    if "medicines" not in tgt["optional_includes"]:
        if meds:
            log_warn("sensitive metrics dropped (not in optional_includes)",
                     count=len(meds), metrics="medications", categories="medicines")
        return out
    for m in meds:
        # 'start'/'end' are when the dose was LOGGED (identical for a point-in-time
        # "Taken" action); 'scheduledDate' is when it was DUE. The event's own time
        # is the logged moment; scheduled_time is kept as a field so scheduled-vs-
        # actual can be compared in Splunk.
        epoch, day = parse_dt(m.get("start") or m.get("scheduledDate"))
        sched_epoch, _ = parse_dt(m.get("scheduledDate"))
        rxnorm = None
        for c in (m.get("codings") or []):
            if isinstance(c, dict) and "rxnorm" in str(c.get("system", "")).lower():
                rxnorm = c.get("code")
                break
        ev = {
            "medication_name": m.get("displayText"),
            "status": m.get("status"),
            "dosage": m.get("dosage"),
            "scheduled_dosage": m.get("scheduledDosage"),
            "dosage_units": m.get("units"),
            "scheduled_time": sched_epoch,
            "is_archived": bool(m.get("isArchived", False)),
            "day": day,
            "_vendor": "apple",
        }
        if rxnorm:
            ev["rxnorm_code"] = rxnorm
        out.append(("apple:medications", epoch, ev))
    return out


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

    try:
        sync(source, targets, file_override=args.file, dry_run=args.dry_run, keep=args.keep)
    except Exception:
        # sync() already logged the failure via log_error before re-raising
        sys.exit(1)


def sync(source, targets, file_override=None, dry_run=False, keep=False):
    """
    Run one Apple Health ingest pass (discover-or-use-file, explode, per-target
    dedup+send, archive/delete) and return {"sent", "skipped", "files", "failed"}.

    Extracted out of main() so a future master orchestrator (or an
    embedded-interpreter mobile build) can call this directly in-process for
    one vendor among several. main() still does argparse, --status/
    --reset-dedup handling, the --person testing override, and the
    single-instance file lock; this assumes all of that already happened and
    `source`/`targets` are the final resolved values (including any --person
    override already applied).
    """
    store = load_json(DEDUP_FILE, {})
    configure_hec_log(load_logging_cfg(), targets, dry_run)
    files = [Path(file_override)] if file_override else discover_files(source)
    t0 = time.time()
    log_info("run started", fetcher_ver=FETCHER_VERSION, run_host=RUN_HOST, watch_dir=source["watch_dir"], targets=len(targets),
             files=len(files), dry_run=dry_run)
    if not files:
        log_info("run complete", events=0, skipped=0, files=0, failures=0, targets=len(targets),
                 duration_s=round(time.time() - t0, 1), dry_run=dry_run)
        return {"sent": 0, "skipped": 0, "files": 0, "failed": 0}

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
                    if dry_run:
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
            if file_ok and not dry_run:
                finish_file(source, p, dry=keep)
                if not keep:
                    log_info("finished file", file=p.name,
                             action=("archived" if source["archive_dir"] else "deleted"))
    except Exception as e:
        log_error("run failed", error=type(e).__name__, detail=str(e),
                  duration_s=round(time.time() - t0, 1))
        raise

    # prune + persist dedup store
    for n, d in store.items():
        if len(d) > DEDUP_MAX:
            store[n] = dict(list(d.items())[-DEDUP_MAX:])
    if not dry_run:
        save_json(DEDUP_FILE, store)
    # Discovery: one WARN per unknown relayed source this run (promote into VENDOR_KEYWORDS).
    for src, n in sorted(_UNKNOWN_SOURCES.items(), key=lambda kv: -kv[1]):
        log_warn("unknown relayed source", raw_source=src, vendor=_provisional_vendor(src),
                 via="apple", points=n)
    log_info("run complete", events=total_sent, skipped=total_skip, files=len(files),
             failures=failed, targets=len(targets), duration_s=round(time.time() - t0, 1),
             dry_run=dry_run)
    flush_hec_log()
    return {"sent": total_sent, "skipped": total_skip, "files": len(files), "failed": failed}


if __name__ == "__main__":
    main()
