#!/usr/bin/env python3
"""Voyager DSN + PHNL wind watcher.

Designed to run one cycle per invocation (``--once``) from a scheduled
GitHub Actions workflow, with all state kept in ``state/`` and committed
back to the repository. Each cycle snapshots NASA's DSN Now feed for
Voyager 1/2 contacts (and any dishes the feed marks offline or anomalous)
and, when at least 30 minutes have passed since the last fetch, the PHNL
METAR. Results are appended as JSON lines and summarized in summary.md.

Network errors and placeholder ("blank") DSN feeds are logged as failed
cycles; they are never treated as a Voyager signal loss.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import math
import os
import re
import signal
import sys
import time
import traceback
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

CODE_DIR = Path(__file__).resolve().parent
DIR = Path(os.environ.get("WATCHER_STATE_DIR") or (CODE_DIR / "state")).resolve()
DSN_JSONL = DIR / "dsn.jsonl"
METAR_JSONL = DIR / "phnl_metar.jsonl"
SUMMARY_MD = DIR / "summary.md"
LOG_PATH = DIR / "watcher.log"
PID_PATH = DIR / "watcher.pid"
LOCK_PATH = DIR / "watcher.lock"
WAKES_LOG = DIR / "wakes.log"
WAKE_STATE_PATH = DIR / "wake_state.json"
ENV_PATH = Path(os.environ.get("WATCHER_ENV_FILE") or (CODE_DIR / ".env"))
KEEPALIVE_PATH = DIR / "keepalive.txt"

DSN_URL = "https://eyes.nasa.gov/dsn/data/dsn.xml"
METAR_URL = "https://aviationweather.gov/api/data/metar?ids=PHNL&format=json"
USER_AGENT = "voyager-watcher/2.0 (+https://github.com/calebxia-pilot/voyager-watcher)"
HTTP_TIMEOUT = 25

DSN_INTERVAL_S = 5 * 60
METAR_INTERVAL_S = 30 * 60
LOG_ROTATE_BYTES = 50 * 1024 * 1024
JSONL_ROTATE_BYTES = 50 * 1024 * 1024
RETENTION_DAYS = 14  # trim jsonl/wakes.log to this many days each cycle
# Placeholder-feed heuristic: this many dishes or more, all parked at the
# same az/el with the same activity text and no live target or signal.
PLACEHOLDER_MIN_DISHES = 5
SESSION_GAP = timedelta(minutes=15)
POWER_DIP_DB = 3.0
CROSSWIND_WAKE_KT = 15.0
WAKE_MAX_PER_24H = 4
WAKE_MIN_GAP = timedelta(hours=2)
WAKE_SAME_TYPE_GAP = timedelta(hours=6)
WAKE_QUIET_START = 22  # 10 PM America/Edmonton, inclusive
WAKE_QUIET_END = 7     # 7 AM America/Edmonton, exclusive (flush at 07:00)
# Lower number = more important, used to pick one overnight queued wake.
WAKE_PRIORITY = {
    "voyager_lost": 1,
    "dsn_anomaly": 2,
    "power_dip": 3,
    "voyager_acquired": 4,
    "unexpected_antenna": 5,
    "phnl_crosswind": 6,
}

EDMONTON = ZoneInfo("America/Edmonton")
UTC = timezone.utc

# PHNL runways we care about for crosswind (true headings, approx).
RUNWAYS = {
    "08R": 80,
    "26L": 260,
    "04R": 40,
    "22L": 220,
}

VOYAGER_RE = re.compile(
    r"^(?:VGR|VG|VOYAGER[\s\-_]?)([12])$",
    re.IGNORECASE,
)

OFFLINE_HINTS = (
    "upgrade",
    "maintenance",
    "offline",
    "outage",
    "repair",
    "inoperable",
    "inoperative",
    "anomal",
    "failed",
    "failure",
    "disabled",
    "not operational",
    "calibration",
    "out of service",
)

log = logging.getLogger("watcher")
_lock_fh = None
_stop = False


def utcnow() -> datetime:
    return datetime.now(UTC)


def iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(UTC)
    except (ValueError, TypeError):
        return None


def ms_to_dt(ms: Any) -> Optional[datetime]:
    try:
        val = float(ms)
        if val > 1e12:
            val = val / 1000.0
        if val <= 0:
            return None
        return datetime.fromtimestamp(val, tz=UTC)
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def setup_logging(to_stdout: bool = False) -> None:
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    if to_stdout:
        # One-shot / CI mode: the job log is the process log.
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        log.addHandler(sh)
        return
    fh = RotatingFileHandler(
        LOG_PATH, maxBytes=LOG_ROTATE_BYTES, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    log.addHandler(fh)
    # Only echo to stderr when it is a terminal, so nohup+FileHandler
    # do not write the same line twice to watcher.log.
    if sys.stderr.isatty():
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        log.addHandler(sh)


def acquire_lock_or_exit() -> None:
    global _lock_fh
    _lock_fh = open(LOCK_PATH, "a+", encoding="utf-8")
    try:
        fcntl.flock(_lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log.info("another watcher is already running; exiting")
        sys.exit(0)
    _lock_fh.seek(0)
    _lock_fh.truncate()
    _lock_fh.write(str(os.getpid()))
    _lock_fh.flush()
    PID_PATH.write_text(str(os.getpid()) + "\n", encoding="utf-8")


def release_lock() -> None:
    try:
        if PID_PATH.exists():
            try:
                if PID_PATH.read_text(encoding="utf-8").strip() == str(os.getpid()):
                    PID_PATH.unlink()
            except OSError:
                pass
        if _lock_fh is not None:
            fcntl.flock(_lock_fh.fileno(), fcntl.LOCK_UN)
            _lock_fh.close()
    except OSError:
        pass


def handle_stop(signum, _frame) -> None:
    global _stop
    _stop = True
    log.info("received signal %s; stopping after this cycle", signum)


def http_get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return resp.read()


def rotate_if_huge(path: Path) -> None:
    try:
        if not path.exists() or path.stat().st_size <= JSONL_ROTATE_BYTES:
            return
        stamp = utcnow().strftime("%Y%m%dT%H%M%SZ")
        dest = path.with_name(f"{path.name}.{stamp}")
        path.rename(dest)
        log.info("rotated %s -> %s (exceeded 50 MB)", path.name, dest.name)
    except OSError as exc:
        log.warning("could not rotate %s: %s", path, exc)


def append_jsonl(path: Path, record: dict) -> None:
    rotate_if_huge(path)
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
    with path.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def normalize_dish(name: str) -> str:
    raw = (name or "").strip().upper().replace("_", "")
    m = re.match(r"^(DSS)[- ]?(\d+)$", raw)
    if m:
        return f"DSS-{int(m.group(2))}"
    return name.strip() if name else "unknown"


def normalize_spacecraft(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    m = VOYAGER_RE.match(name.strip())
    if not m:
        return None
    return f"VGR{m.group(1)}"


def attr(el: ET.Element, key: str, default: str = "") -> str:
    return (el.get(key) or default).strip()


def as_float(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def as_bool(val: Any) -> Optional[bool]:
    if val is None or val == "":
        return None
    s = str(val).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


def activity_flag(activity: str) -> Optional[str]:
    text = (activity or "").lower()
    if not text:
        return None
    if any(h in text for h in OFFLINE_HINTS):
        if any(h in text for h in ("anomal", "failed", "failure")):
            return "anomalous"
        return "offline"
    return None


def collect_signals(dish: ET.Element) -> list[dict]:
    out = []
    for tag, direction in (("upSignal", "up"), ("downSignal", "down")):
        for sig in dish.findall(tag):
            out.append(
                {
                    "direction": direction,
                    "active": as_bool(attr(sig, "active")) or False,
                    "signal_type": attr(sig, "signalType") or None,
                    "data_rate_bps": as_float(attr(sig, "dataRate")),
                    "power_dbm": as_float(attr(sig, "power")),
                    "band": attr(sig, "band") or None,
                    "frequency_hz": as_float(attr(sig, "frequency")),
                    "spacecraft_raw": attr(sig, "spacecraft") or None,
                    "spacecraft": normalize_spacecraft(attr(sig, "spacecraft")),
                }
            )
    return out


def parse_dsn(xml_bytes: bytes, fetched_at: datetime) -> dict:
    root = ET.fromstring(xml_bytes)
    feed_ts = ms_to_dt(root.findtext("timestamp"))
    station_code = None
    station_name = None
    # Prefer a station-level clock if the root timestamp is missing.
    contacts: list[dict] = []
    anomalous: list[dict] = []
    dish_shapes: list[tuple] = []

    for child in list(root):
        tag = child.tag
        if tag == "station":
            station_code = attr(child, "name") or None
            station_name = attr(child, "friendlyName") or station_code
            if feed_ts is None:
                feed_ts = ms_to_dt(attr(child, "timeUTC"))
            continue
        if tag != "dish":
            continue

        dish_name = normalize_dish(attr(child, "name"))
        activity = attr(child, "activity")
        flag = activity_flag(activity)
        target_el = child.find("target")
        target_raw = attr(target_el, "name") if target_el is not None else ""
        target_sc = normalize_spacecraft(target_raw)
        rtlt = as_float(attr(target_el, "rtlt")) if target_el is not None else None
        if rtlt is not None and rtlt < 0:
            rtlt = None

        signals = collect_signals(child)
        dish_shapes.append(
            (
                attr(child, "azimuthAngle"),
                attr(child, "elevationAngle"),
                activity,
                target_raw,
                rtlt is None,
                any(s.get("active") for s in signals),
            )
        )
        voyager_from_sigs = {s["spacecraft"] for s in signals if s.get("spacecraft")}
        tracking = sorted(s for s in ({target_sc} | voyager_from_sigs) if s)

        dish_meta = {
            "dish": dish_name,
            "station": station_name,
            "station_code": station_code,
            "activity": activity or None,
            "azimuth_deg": as_float(attr(child, "azimuthAngle")),
            "elevation_deg": as_float(attr(child, "elevationAngle")),
            "wind_speed": as_float(attr(child, "windSpeed")),
        }

        if flag:
            anomalous.append({**dish_meta, "flag": flag, "target": target_raw or None})

        for sc in tracking:
            sc_signals = [
                {k: v for k, v in s.items() if k != "spacecraft_raw"}
                for s in signals
                if s.get("spacecraft") == sc
                or (not s.get("spacecraft") and target_sc == sc)
            ]
            # If the target is Voyager but signals are unlabeled, keep them.
            if not sc_signals and target_sc == sc:
                sc_signals = [
                    {k: v for k, v in s.items() if k != "spacecraft_raw"}
                    for s in signals
                ]
            contacts.append(
                {
                    **dish_meta,
                    "spacecraft": sc,
                    "rtlt_s": rtlt,
                    "target": target_raw or sc,
                    "signals": sc_signals,
                    "signal_active": any(s.get("active") for s in sc_signals),
                }
            )

    placeholder = placeholder_reason(dish_shapes)
    if placeholder:
        # A blank/placeholder feed is "feed down", never "Voyager lost".
        return {
            "ts_utc": iso(fetched_at),
            "feed_ts_utc": iso(feed_ts) if feed_ts else None,
            "ok": False,
            "feed_down": True,
            "error": f"feed down: {placeholder}",
            "contacts": [],
            "anomalous_or_offline": [],
        }
    return {
        "ts_utc": iso(fetched_at),
        "feed_ts_utc": iso(feed_ts) if feed_ts else None,
        "ok": True,
        "error": None,
        "contacts": contacts,
        "anomalous_or_offline": anomalous,
    }


def placeholder_reason(shapes: list[tuple]) -> Optional[str]:
    """Return a description if the DSN feed looks blank/placeholder.

    Seen 2026-11-11: every dish parked at az 0 / el 90, all with the same
    activity text, target "DSN" and rtlt -1, no active signals. That is a
    feed/pipeline outage, not the network genuinely tracking nothing.
    """
    if not shapes:
        return "no dishes in DSN feed"
    if len(shapes) < PLACEHOLDER_MIN_DISHES:
        return None
    if len(set(shapes)) != 1:
        return None
    az, el, activity, target, no_rtlt, any_active = shapes[0]
    if any_active or not no_rtlt:
        return None
    return (
        f"all {len(shapes)} dishes in the same placeholder state "
        f"(az {az or '?'}, el {el or '?'}, activity '{activity or '-'}', target '{target or '-'}')"
    )


def is_feed_down(rec: Optional[dict]) -> bool:
    """True for placeholder records, including ones logged before detection existed."""
    if not rec:
        return False
    if rec.get("feed_down"):
        return True
    if not rec.get("ok") or rec.get("contacts"):
        return False
    dishes = rec.get("anomalous_or_offline") or []
    if len(dishes) < PLACEHOLDER_MIN_DISHES:
        return False
    shapes = {
        (d.get("azimuth_deg"), d.get("elevation_deg"), d.get("activity"), d.get("target"))
        for d in dishes
    }
    return len(shapes) == 1


def fetch_dsn() -> dict:
    fetched_at = utcnow()
    try:
        raw = http_get(DSN_URL)
        record = parse_dsn(raw, fetched_at)
        if record.get("feed_down"):
            log.warning("DSN %s", record["error"])
            return record
        n = len(record["contacts"])
        offline = len(record["anomalous_or_offline"])
        scs = ", ".join(
            sorted({f"{c['spacecraft']}@{c['dish']}" for c in record["contacts"]})
        ) or "none"
        log.info("DSN ok: %d Voyager contact(s) [%s], %d offline/anomalous", n, scs, offline)
        return record
    except Exception as exc:
        log.warning("DSN fetch/parse failed: %s", exc)
        return {
            "ts_utc": iso(fetched_at),
            "feed_ts_utc": None,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "contacts": [],
            "anomalous_or_offline": [],
        }


def ceiling_from_clouds(clouds: Any) -> Optional[int]:
    if not isinstance(clouds, list):
        return None
    bases = []
    for layer in clouds:
        if not isinstance(layer, dict):
            continue
        cover = str(layer.get("cover") or "").upper()
        if cover in ("BKN", "OVC", "VV"):
            try:
                bases.append(int(layer["base"]))
            except (KeyError, TypeError, ValueError):
                continue
    return min(bases) if bases else None


def fetch_metar() -> dict:
    fetched_at = utcnow()
    try:
        raw = http_get(METAR_URL)
        data = json.loads(raw.decode("utf-8"))
        if isinstance(data, list):
            obs = data[0] if data else {}
        elif isinstance(data, dict):
            obs = data
        else:
            obs = {}
        if not obs:
            raise ValueError("empty METAR payload")
        clouds = obs.get("clouds") or []
        record = {
            "ts_utc": iso(fetched_at),
            "ok": True,
            "error": None,
            "obs_time_utc": obs.get("reportTime") or iso(ms_to_dt(obs.get("obsTime"))),
            "wind_dir_deg": as_float(obs.get("wdir")),
            "wind_speed_kt": as_float(obs.get("wspd")),
            "wind_gust_kt": as_float(obs.get("wgst")),
            "visibility": obs.get("visib"),
            "ceiling_ft": ceiling_from_clouds(clouds),
            "cover": obs.get("cover"),
            "clouds": clouds,
            "temp_c": as_float(obs.get("temp")),
            "dewpoint_c": as_float(obs.get("dewp")),
            "altim_hpa": as_float(obs.get("altim")),
            "flight_category": obs.get("fltCat"),
            "raw": obs.get("rawOb") or obs.get("raw") or "",
        }
        log.info(
            "METAR ok: wind %s/%sG%s vis %s ceiling %s",
            record["wind_dir_deg"],
            record["wind_speed_kt"],
            record["wind_gust_kt"],
            record["visibility"],
            record["ceiling_ft"],
        )
        return record
    except Exception as exc:
        log.warning("METAR fetch/parse failed: %s", exc)
        return {
            "ts_utc": iso(fetched_at),
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "obs_time_utc": None,
            "wind_dir_deg": None,
            "wind_speed_kt": None,
            "wind_gust_kt": None,
            "visibility": None,
            "ceiling_ft": None,
            "cover": None,
            "clouds": [],
            "temp_c": None,
            "dewpoint_c": None,
            "altim_hpa": None,
            "flight_category": None,
            "raw": "",
        }


def wind_components(wind_dir: Optional[float], speed: Optional[float], rwy_hdg: int) -> dict:
    if wind_dir is None or speed is None:
        return {"cross_kt": None, "head_kt": None, "cross_from": None}
    delta = math.radians(wind_dir - rwy_hdg)
    cross = speed * math.sin(delta)  # + from the right
    head = speed * math.cos(delta)  # + headwind
    if abs(cross) < 0.05:
        side = "calm"
    elif cross > 0:
        side = "right"
    else:
        side = "left"
    return {
        "cross_kt": round(abs(cross), 1),
        "head_kt": round(head, 1),
        "cross_from": side,
    }


def read_jsonl_window(paths: list[Path], since: datetime) -> list[dict]:
    rows = []
    for path in paths:
        if not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    ts = parse_iso(rec.get("ts_utc") or rec.get("feed_ts_utc"))
                    if ts is None or ts >= since:
                        rows.append(rec)
        except OSError as exc:
            log.warning("could not read %s: %s", path, exc)
    rows.sort(key=lambda r: r.get("ts_utc") or "")
    return rows


def rotated_siblings(path: Path) -> list[Path]:
    found = []
    if not path.parent.exists():
        return found
    for p in path.parent.glob(path.name + "*"):
        if p == path or p.name.startswith(path.name + "."):
            found.append(p)
    # Newest rotations first isn't required; read_jsonl_window filters by time.
    return sorted(set(found))


def fmt_rate(bps: Optional[float]) -> str:
    if bps is None:
        return "—"
    if bps >= 1_000_000:
        return f"{bps/1_000_000:.3g} Mbps"
    if bps >= 1000:
        return f"{bps/1000:.3g} kbps"
    if bps == 0:
        return "0 bps"
    return f"{int(bps) if bps == int(bps) else bps} bps"


def fmt_power(dbm: Optional[float]) -> str:
    if dbm is None:
        return "—"
    return f"{dbm:g} dBm"


def fmt_local(dt: Optional[datetime]) -> str:
    if dt is None:
        return "—"
    local = dt.astimezone(EDMONTON)
    return local.strftime("%Y-%m-%d %H:%M") + " America/Edmonton (" + iso(dt) + ")"


def primary_downlink(contact: dict) -> dict:
    signals = contact.get("signals") or []
    downs = [s for s in signals if s.get("direction") == "down"]
    active = [s for s in downs if s.get("active")]
    pool = active or downs or signals
    if not pool:
        return {}
    # Prefer the signal with a non-zero data rate, then strongest (least negative) power.
    def key(s: dict) -> tuple:
        rate = s.get("data_rate_bps") or 0
        power = s.get("power_dbm")
        power_ord = power if power is not None else -9999
        return (rate, power_ord)

    return max(pool, key=key)


def build_sessions(records: list[dict]) -> list[dict]:
    """Group consecutive same-spacecraft/dish contacts into sessions."""
    # key -> open session
    open_sess: dict[tuple, dict] = {}
    done: list[dict] = []

    def close(key: tuple) -> None:
        sess = open_sess.pop(key, None)
        if sess:
            done.append(sess)

    for rec in records:
        ts = parse_iso(rec.get("feed_ts_utc") or rec.get("ts_utc"))
        if ts is None or not rec.get("ok") or is_feed_down(rec):
            continue
        seen = set()
        for c in rec.get("contacts") or []:
            key = (c.get("spacecraft"), c.get("dish"))
            seen.add(key)
            sig = primary_downlink(c)
            rate = sig.get("data_rate_bps")
            power = sig.get("power_dbm")
            active = bool(c.get("signal_active") or sig.get("active"))
            if key in open_sess:
                sess = open_sess[key]
                if ts - sess["end"] <= SESSION_GAP:
                    sess["end"] = ts
                    sess["samples"] += 1
                    if rate is not None:
                        sess["rates"].append(rate)
                    if power is not None:
                        sess["powers"].append(power)
                    sess["last_active"] = active
                    sess["last_band"] = sig.get("band") or sess["last_band"]
                    sess["station"] = c.get("station") or sess["station"]
                    continue
                close(key)
            open_sess[key] = {
                "spacecraft": c.get("spacecraft"),
                "dish": c.get("dish"),
                "station": c.get("station"),
                "start": ts,
                "end": ts,
                "samples": 1,
                "rates": [rate] if rate is not None else [],
                "powers": [power] if power is not None else [],
                "last_active": active,
                "last_band": sig.get("band"),
            }
        # Close sessions missing from this successful snapshot (beyond the gap,
        # which we handle on the next sighting; if they disappeared, close now).
        for key in list(open_sess):
            if key not in seen and ts - open_sess[key]["end"] > SESSION_GAP:
                close(key)

    done.extend(open_sess.values())
    done.sort(key=lambda s: (s["spacecraft"] or "", s["start"]))
    return done


def build_events(records: list[dict]) -> list[str]:
    events: list[str] = []
    prev_keys: set[tuple] = set()
    prev_power: dict[tuple, float] = {}
    seen_antennas: dict[str, set] = {"VGR1": set(), "VGR2": set()}
    last_ok_ts: Optional[datetime] = None
    fail_run: Optional[dict] = None

    def flush_fail(until: Optional[datetime]) -> None:
        nonlocal fail_run
        if not fail_run:
            return
        kind = "DSN feed down" if fail_run["feed_down"] else "DSN fetch failed"
        span = fmt_local(fail_run["start"])
        if fail_run["n"] > 1:
            span += f" → {fmt_local(fail_run['end'])}"
        tail = f" ({fail_run['n']} cycle(s); {fail_run['error']})"
        tail += "; feed back" if until is not None else "; still down"
        events.append(f"- {span} — {kind}{tail}")
        fail_run = None

    for rec in records:
        ts = parse_iso(rec.get("feed_ts_utc") or rec.get("ts_utc"))
        if ts is None:
            continue
        if not rec.get("ok") or is_feed_down(rec):
            down = is_feed_down(rec)
            err = rec.get("error") or ("feed down: placeholder data" if down else "unknown error")
            if fail_run is None:
                fail_run = {"start": ts, "end": ts, "n": 0, "feed_down": down, "error": err}
            fail_run["end"] = ts
            fail_run["error"] = err
            fail_run["n"] += 1
            fail_run["feed_down"] = fail_run["feed_down"] or down
            continue
        recovering = fail_run is not None
        flush_fail(ts)

        current = {}
        for c in rec.get("contacts") or []:
            sc, dish = c.get("spacecraft"), c.get("dish")
            if not sc or not dish:
                continue
            sig = primary_downlink(c)
            if not is_locked(c.get("signal_active") or sig.get("active"), sig.get("power_dbm")):
                continue  # pointed/setup only, not a lock
            current[(sc, dish)] = {
                "active": bool(c.get("signal_active") or sig.get("active")),
                "power": sig.get("power_dbm"),
                "rate": sig.get("data_rate_bps"),
                "band": sig.get("band"),
                "station": c.get("station"),
            }

        current_keys = set(current)
        # After a failed fetch, don't treat reappearance as a fresh acquire
        # unless we already had a previous successful snapshot. Right after a
        # feed outage, don't call anything "lost": we can't know when it ended.
        if recovering:
            prev_keys = prev_keys & current_keys
        if last_ok_ts is not None:
            for key in sorted(current_keys - prev_keys):
                sc, dish = key
                info = current[key]
                if dish not in seen_antennas.get(sc, set()) and seen_antennas.get(sc):
                    kind = "new antenna"
                else:
                    kind = "signal acquired"
                rate = fmt_rate(info["rate"])
                pwr = fmt_power(info["power"])
                events.append(
                    f"- {fmt_local(ts)} — {sc} {kind} on {dish}"
                    f" ({info.get('station') or '?'}, {info.get('band') or '?'} {rate}, {pwr})"
                )
            for key in sorted(prev_keys - current_keys):
                sc, dish = key
                events.append(f"- {fmt_local(ts)} — {sc} signal lost on {dish}")

            for key, info in current.items():
                pwr = info.get("power")
                prev = prev_power.get(key)
                if pwr is not None and prev is not None and (prev - pwr) > POWER_DIP_DB:
                    sc, dish = key
                    events.append(
                        f"- {fmt_local(ts)} — {sc} power dip on {dish}: "
                        f"{prev:g} → {pwr:g} dBm ({prev - pwr:.1f} dB)"
                    )

        for key, info in current.items():
            sc, dish = key
            seen_antennas.setdefault(sc, set()).add(dish)
            if info.get("power") is not None:
                prev_power[key] = info["power"]
        # Drop power memory for dishes no longer tracking, so a later re-acquire
        # doesn't compare against a stale value from hours ago.
        for key in list(prev_power):
            if key not in current:
                prev_power.pop(key, None)

        prev_keys = current_keys
        last_ok_ts = ts

    flush_fail(None)
    return events


def format_session(sess: dict) -> str:
    rates = sess.get("rates") or []
    if rates:
        nonzero = [r for r in rates if r]
        typical = (nonzero or rates)[-1]
        rmin, rmax = min(rates), max(rates)
        if rmin == rmax:
            rate_s = fmt_rate(typical)
        else:
            rate_s = f"{fmt_rate(rmin)}–{fmt_rate(rmax)} (last {fmt_rate(typical)})"
    else:
        rate_s = "rate unknown"
    band = sess.get("last_band") or "?"
    powers = sess.get("powers") or []
    pwr_s = f", {fmt_power(powers[-1])}" if powers else ""
    start, end = sess["start"], sess["end"]
    same_min = start.astimezone(EDMONTON).strftime("%H:%M") == end.astimezone(EDMONTON).strftime("%H:%M")
    start_s = fmt_local(start)
    end_s = "ongoing" if (utcnow() - end) < timedelta(minutes=12) else fmt_local(end)
    if same_min and end_s != "ongoing":
        span = start_s
    else:
        span = f"{start_s} → {end_s}"
    station = sess.get("station") or "?"
    return f"- {sess['dish']} ({station}): {span}; {band}-band {rate_s}{pwr_s}; {sess['samples']} sample(s)"


def format_wind_block(metar: Optional[dict]) -> str:
    if not metar:
        return "No PHNL METAR recorded yet.\n"
    if not metar.get("ok"):
        return f"Latest METAR fetch failed at {fmt_local(parse_iso(metar.get('ts_utc')))}: {metar.get('error')}\n"

    ts = parse_iso(metar.get("obs_time_utc") or metar.get("ts_utc"))
    direc = metar.get("wind_dir_deg")
    spd = metar.get("wind_speed_kt")
    gust = metar.get("wind_gust_kt")
    vis = metar.get("visibility")
    ceiling = metar.get("ceiling_ft")
    raw = metar.get("raw") or ""

    if direc is None and spd is None:
        wind_s = "wind missing"
    else:
        dir_s = f"{int(direc):03d}°" if direc is not None else "VRB"
        if spd is None:
            wind_s = f"{dir_s} at unknown speed"
        elif gust:
            wind_s = f"{dir_s} at {spd:g} kt, gusting {gust:g} kt"
        else:
            wind_s = f"{dir_s} at {spd:g} kt"

    ceil_s = f"{int(ceiling)} ft" if ceiling is not None else "none (no BKN/OVC layer)"
    vis_s = f"{vis} SM" if vis is not None else "unknown"

    lines = [
        f"Observed: {fmt_local(ts)}",
        f"Wind: {wind_s}",
        f"Visibility: {vis_s}",
        f"Ceiling: {ceil_s}",
    ]
    if raw:
        lines.append(f"Raw: `{raw}`")
    lines.append("")
    lines.append("Crosswind / headwind for PHNL 26L/08R (~260°/080°) and 04R/22L (~040°/220°).")
    lines.append("Crosswind is the component from the left or right of the nose; headwind is positive, tailwind negative.")
    if gust:
        lines.append("Gust values in parentheses.")
    lines.append("")
    for name, hdg in RUNWAYS.items():
        c = wind_components(direc, spd, hdg)
        if c["cross_kt"] is None:
            lines.append(f"- {name} ({hdg:03d}°): unavailable")
            continue
        head = c["head_kt"]
        hw = f"headwind {head:g} kt" if head >= 0 else f"tailwind {abs(head):g} kt"
        side = c["cross_from"]
        cross_s = "no crosswind" if side == "calm" else f"crosswind {c['cross_kt']:g} kt from the {side}"
        extra = ""
        if gust is not None:
            g = wind_components(direc, gust, hdg)
            ghw = f"HW {g['head_kt']:g}" if g["head_kt"] >= 0 else f"TW {abs(g['head_kt']):g}"
            extra = f" (gust: {g['cross_kt']:g} kt cross, {ghw} kt)"
        lines.append(f"- {name} ({hdg:03d}°): {cross_s}, {hw}{extra}")
    return "\n".join(lines) + "\n"


def write_summary() -> None:
    now = utcnow()
    since = now - timedelta(hours=24)
    dsn_rows = read_jsonl_window(rotated_siblings(DSN_JSONL), since)
    metar_rows = read_jsonl_window(rotated_siblings(METAR_JSONL), since)
    latest_metar = None
    for row in reversed(metar_rows):
        latest_metar = row
        break
    # Prefer the most recent successful METAR if the last one failed.
    for row in reversed(metar_rows):
        if row.get("ok"):
            latest_metar = row
            break

    sessions = build_sessions(dsn_rows)
    events = build_events(dsn_rows)

    v1 = [s for s in sessions if s.get("spacecraft") == "VGR1"]
    v2 = [s for s in sessions if s.get("spacecraft") == "VGR2"]

    def section(title: str, items: list[dict]) -> list[str]:
        if not items:
            return [f"### {title}", "No contacts in the last 24 hours.", ""]
        return [f"### {title}", *[format_session(s) for s in items], ""]

    last_ok = next((r for r in reversed(dsn_rows) if r.get("ok") and not is_feed_down(r)), None)
    last_any = dsn_rows[-1] if dsn_rows else None
    offline_lines = []
    if last_ok:
        for d in last_ok.get("anomalous_or_offline") or []:
            offline_lines.append(
                f"- {d.get('dish')} ({d.get('station') or '?'}): {d.get('flag')} — {d.get('activity') or 'no activity text'}"
            )

    lines = [
        "# DSN / PHNL watcher summary",
        "",
        f"Generated: {fmt_local(now)}",
        "Window: last 24 hours. Times below are America/Edmonton with UTC in parentheses.",
        "",
        "## Voyager contacts — last 24 hours",
        "",
        *section("Voyager 1 (VGR1)", v1),
        *section("Voyager 2 (VGR2)", v2),
        "## Notable events",
        "",
    ]
    if events:
        lines.extend(events)
        lines.append("")
    else:
        lines.append("No acquire/loss events, power dips >3 dB, or new antennas in this window yet.")
        lines.append("")

    lines.extend(["## Offline / anomalous dishes (latest snapshot)", ""])
    if offline_lines:
        lines.extend(offline_lines)
        lines.append("")
    elif last_ok:
        lines.extend(["None flagged in the latest successful snapshot.", ""])
    else:
        lines.extend(["No successful DSN snapshot yet.", ""])

    if last_any and (not last_any.get("ok") or is_feed_down(last_any)):
        if is_feed_down(last_any):
            since_row = None
            for r in reversed(dsn_rows):
                if is_feed_down(r) or not r.get("ok"):
                    since_row = r
                    continue
                break
            since_ts = parse_iso((since_row or last_any).get("ts_utc"))
            lines.append(
                f"Note: the DSN Now feed is DOWN (placeholder data) since {fmt_local(since_ts)}. "
                "Offline list above is from the last real snapshot. This is not a Voyager signal loss."
            )
        else:
            lines.append(f"Note: the latest DSN attempt failed: {last_any.get('error')}")
        lines.append("")

    lines.extend(["## Latest PHNL wind", "", format_wind_block(latest_metar)])

    tmp = SUMMARY_MD.with_suffix(".md.tmp")
    tmp.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    tmp.replace(SUMMARY_MD)



def webhook_auth_headers(key: str) -> dict[str, str]:
    """Build webhook auth headers. Change the scheme in this function only."""
    return {
        "Authorization": f"Bearer {key}",
        "X-Webhook-Key": key,
    }


def load_webhook_config() -> tuple[Optional[str], Optional[str]]:
    """Return (url, key). Process env, then overlay watcher/.env if present.

    Re-read every cycle so a new URL or key takes effect without a restart.
    """
    env = dict(os.environ)
    if ENV_PATH.exists():
        try:
            for raw in ENV_PATH.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                name, _, value = line.partition("=")
                name = name.strip()
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                    value = value[1:-1]
                env[name] = value
        except OSError as exc:
            log.warning("could not read %s: %s", ENV_PATH, exc)
    url = (env.get("CALEB_WATCHER_WEBHOOK_URL") or "").strip() or None
    key = (env.get("CALEB_WATCHER_WEBHOOK_KEY") or "").strip() or None
    return url, key


def append_wakes_log(line: str) -> None:
    rotate_if_huge(WAKES_LOG)
    try:
        with WAKES_LOG.open("a", encoding="utf-8") as fh:
            fh.write(line.rstrip() + "\n")
    except OSError as exc:
        log.error("could not write %s: %s", WAKES_LOG, exc)


def log_wake_decision(
    decision: str,
    event: Optional[str],
    detail: Optional[str],
    reason: str,
    time_utc: Optional[str] = None,
) -> None:
    now = utcnow()
    parts = [
        fmt_local(now),
        f"decision={decision}",
    ]
    if event:
        parts.append(f"event={event}")
    if time_utc:
        parts.append(f"time_utc={time_utc}")
    if detail:
        # Keep a decision on one line for easy grepping.
        parts.append(f"detail={detail.replace(chr(10), ' ').strip()}")
    parts.append(f"reason={reason}")
    append_wakes_log(" ".join(parts))
    log.info("wake %s %s — %s", decision, event or "-", reason)


def load_wake_state() -> dict:
    if not WAKE_STATE_PATH.exists():
        return {"version": 1, "last_dsn": None, "sent": [], "queue": None, "feed_down_since": None}
    try:
        data = json.loads(WAKE_STATE_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("wake state is not an object")
        data.setdefault("version", 1)
        data.setdefault("last_dsn", None)
        data.setdefault("sent", [])
        data.setdefault("queue", None)
        data.setdefault("feed_down_since", None)
        if not isinstance(data["sent"], list):
            data["sent"] = []
        return data
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        log.warning("could not read wake state (%s); starting fresh", exc)
        return {"version": 1, "last_dsn": None, "sent": [], "queue": None, "feed_down_since": None}


def save_wake_state(state: dict) -> None:
    tmp = WAKE_STATE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp.replace(WAKE_STATE_PATH)


def in_quiet_hours(now: Optional[datetime] = None) -> bool:
    local = (now or utcnow()).astimezone(EDMONTON)
    return local.hour >= WAKE_QUIET_START or local.hour < WAKE_QUIET_END


def next_seven_am(now: Optional[datetime] = None) -> datetime:
    local = (now or utcnow()).astimezone(EDMONTON)
    seven = local.replace(hour=WAKE_QUIET_END, minute=0, second=0, microsecond=0)
    if local < seven:
        return seven
    return seven + timedelta(days=1)


def is_locked(active: Any, power: Any) -> bool:
    """A real downlink lock: DSN says the signal is active and the power is a
    real number, not the feed's -480/-470 dBm "nothing here" placeholder.
    A dish that is only pointed or in setup (active=false) is not a lock."""
    try:
        return bool(active) and power is not None and float(power) > -300.0
    except (TypeError, ValueError):
        return False


def compact_contacts(dsn: dict) -> list[dict]:
    rows = []
    for c in dsn.get("contacts") or []:
        sig = primary_downlink(c)
        rows.append(
            {
                "spacecraft": c.get("spacecraft"),
                "dish": c.get("dish"),
                "station": c.get("station"),
                "signal_active": bool(c.get("signal_active") or sig.get("active")),
                "power_dbm": sig.get("power_dbm"),
                "band": sig.get("band"),
                "data_rate_bps": sig.get("data_rate_bps"),
            }
        )
    return rows


def snapshot_dsn(dsn: dict) -> dict:
    return {
        "ts_utc": dsn.get("feed_ts_utc") or dsn.get("ts_utc"),
        "ok": bool(dsn.get("ok")),
        "contacts": compact_contacts(dsn) if dsn.get("ok") else [],
        "anomalous": [
            {
                "dish": d.get("dish"),
                "station": d.get("station"),
                "flag": d.get("flag"),
                "activity": d.get("activity"),
            }
            for d in (dsn.get("anomalous_or_offline") or [])
            if d.get("flag") == "anomalous"
        ],
    }


def event_ts(dsn: Optional[dict], metar: Optional[dict], fallback: datetime) -> str:
    for src in (dsn, metar):
        if not src:
            continue
        for key in ("feed_ts_utc", "obs_time_utc", "ts_utc"):
            if src.get(key):
                return src[key]
    return iso(fallback) or iso(utcnow()) or ""


def make_event(event: str, detail: str, time_utc: str) -> dict:
    return {
        "event": event,
        "detail": detail,
        "time_utc": time_utc,
        "priority": WAKE_PRIORITY.get(event, 99),
    }


def detect_wake_events(
    prev: Optional[dict],
    dsn: dict,
    metar: Optional[dict],
) -> list[dict]:
    """Return notable events for this cycle, highest priority first.

    DSN transitions need a previous *successful* snapshot so the first look
    (and a restart that already has baseline state) does not wake on the
    current sky. A failed DSN fetch is never treated as a signal loss.
    """
    events: list[dict] = []
    now = utcnow()

    if dsn.get("ok") and prev and prev.get("ok"):
        ts = event_ts(dsn, None, now)
        prev_map = {
            (c.get("spacecraft"), c.get("dish")): c
            for c in (prev.get("contacts") or [])
            if c.get("spacecraft") and c.get("dish")
        }
        curr_map = {
            (c.get("spacecraft"), c.get("dish")): c
            for c in compact_contacts(dsn)
            if c.get("spacecraft") and c.get("dish")
        }
        # Only real locks count for acquired/lost/new-antenna. A dish that is
        # assigned and pointed but not receiving (active=false, -480 dBm) is
        # intent, not signal (false VGR1 "acquired" on DSS-43, 2026-11-13 02:27Z).
        prev_map = {k: c for k, c in prev_map.items()
                    if is_locked(c.get("signal_active"), c.get("power_dbm"))}
        curr_map = {k: c for k, c in curr_map.items()
                    if is_locked(c.get("signal_active"), c.get("power_dbm"))}
        prev_by_sc: dict[str, set] = {}
        curr_by_sc: dict[str, set] = {}
        for sc, dish in prev_map:
            prev_by_sc.setdefault(sc, set()).add(dish)
        for sc, dish in curr_map:
            curr_by_sc.setdefault(sc, set()).add(dish)
        spacecraft = sorted(set(prev_by_sc) | set(curr_by_sc))

        for sc in spacecraft:
            had = prev_by_sc.get(sc, set())
            has = curr_by_sc.get(sc, set())
            if had and not has:
                dishes = ", ".join(sorted(had))
                events.append(
                    make_event(
                        "voyager_lost",
                        f"{sc} signal lost after a gap (was on {dishes}). No dish is tracking it in the latest DSN snapshot.",
                        ts,
                    )
                )
            elif has and not had:
                parts = []
                for dish in sorted(has):
                    c = curr_map[(sc, dish)]
                    parts.append(
                        f"{dish} ({c.get('station') or '?'}, {c.get('band') or '?'} "
                        f"{fmt_rate(c.get('data_rate_bps'))}, {fmt_power(c.get('power_dbm'))})"
                    )
                events.append(
                    make_event(
                        "voyager_acquired",
                        f"{sc} signal acquired after a gap on " + "; ".join(parts) + ".",
                        ts,
                    )
                )
            else:
                # Already on a pass: a new dish joining is an unexpected antenna,
                # not a fresh acquire.
                for dish in sorted(has - had):
                    c = curr_map[(sc, dish)]
                    already = ", ".join(sorted(had)) or "another dish"
                    events.append(
                        make_event(
                            "unexpected_antenna",
                            f"{sc} unexpectedly appeared on {dish} ({c.get('station') or '?'}, "
                            f"{c.get('band') or '?'} {fmt_rate(c.get('data_rate_bps'))}, "
                            f"{fmt_power(c.get('power_dbm'))}) while already tracked on {already}.",
                            ts,
                        )
                    )

        for key, curr in curr_map.items():
            prev_c = prev_map.get(key)
            if not prev_c:
                continue
            prev_p = prev_c.get("power_dbm")
            curr_p = curr.get("power_dbm")
            if prev_p is None or curr_p is None:
                continue
            drop = prev_p - curr_p
            if drop > POWER_DIP_DB:
                sc, dish = key
                events.append(
                    make_event(
                        "power_dip",
                        f"{sc} received-power dip on {dish}: {prev_p:g} → {curr_p:g} dBm "
                        f"({drop:.1f} dB). Band {curr.get('band') or prev_c.get('band') or '?'}, "
                        f"rate {fmt_rate(curr.get('data_rate_bps'))}.",
                        ts,
                    )
                )

        prev_anom = {
            (a.get("dish"), a.get("activity"))
            for a in (prev.get("anomalous") or [])
        }
        prev_anom_dishes = {a.get("dish") for a in (prev.get("anomalous") or [])}
        for a in snapshot_dsn(dsn).get("anomalous") or []:
            dish = a.get("dish")
            # Only wake when a dish newly becomes anomalous, not every cycle
            # it stays that way.
            if dish in prev_anom_dishes:
                continue
            events.append(
                make_event(
                    "dsn_anomaly",
                    f"DSN anomaly on {dish} ({a.get('station') or '?'}): "
                    f"{a.get('activity') or 'flagged anomalous, no activity text'}.",
                    ts,
                )
            )

    if metar and metar.get("ok"):
        ts = event_ts(None, metar, now)
        direc = metar.get("wind_dir_deg")
        spd = metar.get("wind_speed_kt")
        gust = metar.get("wind_gust_kt")
        hits = []
        for name in ("08R", "26L"):
            hdg = RUNWAYS[name]
            sustained = wind_components(direc, spd, hdg)
            gusted = wind_components(direc, gust, hdg) if gust is not None else None
            cross = sustained.get("cross_kt")
            gcross = gusted.get("cross_kt") if gusted else None
            best = max([v for v in (cross, gcross) if v is not None], default=None)
            if best is not None and best >= CROSSWIND_WAKE_KT:
                side = sustained.get("cross_from") or "unknown side"
                bits = [f"{name} ({hdg:03d}°): {cross:g} kt from the {side}"]
                if gcross is not None:
                    bits.append(f"gust component {gcross:g} kt")
                hits.append("; ".join(bits))
        if hits:
            raw = metar.get("raw") or ""
            wind = f"wind {int(direc):03d}° at {spd:g} kt" if direc is not None and spd is not None else "wind missing"
            if gust:
                wind += f", gusting {gust:g} kt"
            detail = (
                "PHNL crosswind component of 15 kt or more on 26L/08R. "
                + " ".join(hits)
                + f". {wind}."
            )
            if raw:
                detail += f" Raw: {raw}"
            events.append(make_event("phnl_crosswind", detail, ts))

    events.sort(key=lambda e: (e["priority"], e["time_utc"]))
    return events


def rate_limit_reason(event: str, state: dict, now: datetime) -> Optional[str]:
    sent = []
    for row in state.get("sent") or []:
        ts = parse_iso(row.get("time_utc") or row.get("sent_utc"))
        if ts is None:
            continue
        sent.append((ts, row.get("event")))
    sent.sort()
    last_24h = [(ts, ev) for ts, ev in sent if now - ts <= timedelta(hours=24)]
    if len(last_24h) >= WAKE_MAX_PER_24H:
        return f"rate limit: already {len(last_24h)} wake(s) in the rolling 24 h (max {WAKE_MAX_PER_24H})"
    if sent:
        last_ts, last_ev = sent[-1]
        gap = now - last_ts
        if gap < WAKE_MIN_GAP:
            remain = WAKE_MIN_GAP - gap
            mins = int(remain.total_seconds() // 60) + 1
            return f"rate limit: minimum {int(WAKE_MIN_GAP.total_seconds()//3600)} h between wakes (last {last_ev} at {iso(last_ts)}, ~{mins} min left)"
    same = [ts for ts, ev in sent if ev == event]
    if same and now - same[-1] < WAKE_SAME_TYPE_GAP:
        return f"rate limit: same event type {event} within {int(WAKE_SAME_TYPE_GAP.total_seconds()//3600)} h (last at {iso(same[-1])})"
    return None


def post_webhook(url: str, key: str, payload: dict) -> tuple[bool, str]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
        **webhook_auth_headers(key),
    }
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return True, f"HTTP {resp.status}"
    except urllib.error.HTTPError as exc:
        body_preview = ""
        try:
            body_preview = exc.read()[:200].decode("utf-8", "replace")
        except Exception:
            pass
        msg = f"HTTP {exc.code}: {exc.reason}"
        if body_preview:
            msg += f" ({body_preview})"
        return False, msg
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def record_sent(state: dict, ev: dict, delivered: bool, now: datetime) -> None:
    state.setdefault("sent", []).append(
        {
            "event": ev["event"],
            "time_utc": iso(now),
            "event_time_utc": ev.get("time_utc"),
            "delivered": delivered,
        }
    )
    # Keep the history short; 48 h is enough for the 24 h window plus margin.
    cutoff = now - timedelta(hours=48)
    kept = []
    for row in state["sent"]:
        ts = parse_iso(row.get("time_utc"))
        if ts is None or ts >= cutoff:
            kept.append(row)
    state["sent"] = kept


def payload_of(ev: dict) -> dict:
    return {
        "event": ev["event"],
        "detail": ev["detail"],
        "time_utc": ev["time_utc"],
    }


def deliver_or_would_wake(ev: dict, url: Optional[str], key: Optional[str], now: datetime) -> tuple[str, str]:
    """Return (decision, reason). decision is sent, would_wake, or failed."""
    if not url or not key:
        missing = []
        if not url:
            missing.append("CALEB_WATCHER_WEBHOOK_URL")
        if not key:
            missing.append("CALEB_WATCHER_WEBHOOK_KEY")
        return "would_wake", "webhook " + " and ".join(missing) + " not set; skipped POST"
    ok, reason = post_webhook(url, key, payload_of(ev))
    if ok:
        return "sent", f"posted to webhook ({reason})"
    return "failed", f"webhook POST failed: {reason}"


def process_wakes(dsn: dict, metar: Optional[dict]) -> None:
    now = utcnow()
    url, key = load_webhook_config()
    state = load_wake_state()
    prev = state.get("last_dsn")

    events = detect_wake_events(prev, dsn, metar)

    # Recovering from a feed outage: the old baseline is stale, so a missing
    # Voyager is "unknown", not "lost". Keep acquisitions and anomalies.
    if dsn.get("ok") and state.get("feed_down_since"):
        dropped = [e for e in events if e["event"] in ("voyager_lost", "power_dip", "unexpected_antenna")]
        if dropped:
            log.info(
                "DSN feed back after outage since %s; not waking on %s (stale baseline)",
                state.get("feed_down_since"),
                ", ".join(e["event"] for e in dropped),
            )
        events = [e for e in events if e not in dropped]
        log.info("DSN feed recovered (down since %s)", state.get("feed_down_since"))
        state["feed_down_since"] = None
    elif dsn.get("feed_down"):
        if not state.get("feed_down_since"):
            state["feed_down_since"] = dsn.get("ts_utc")
            log.info("DSN feed marked down at %s (no wake; not a signal loss)", dsn.get("ts_utc"))

    # Update the baseline only after a successful fetch so a network blip
    # is not recorded as "Voyager disappeared."
    if dsn.get("ok"):
        state["last_dsn"] = snapshot_dsn(dsn)

    # Flush a wake queued during quiet hours (10 PM–7 AM America/Edmonton)
    # on the first cycle at or after 07:00.
    candidates: list[dict] = []
    queued = state.get("queue")
    if queued and not in_quiet_hours(now):
        candidates.append(
            {
                "event": queued["event"],
                "detail": queued.get("detail") or "",
                "time_utc": queued.get("time_utc") or iso(now),
                "priority": WAKE_PRIORITY.get(queued["event"], 99),
                "from_queue": True,
            }
        )
    for ev in events:
        ev = dict(ev)
        ev["from_queue"] = False
        candidates.append(ev)

    sent_this_cycle = False
    for ev in candidates:
        ev_label = ev["event"]
        # If this same event is already the overnight queue and we're still
        # in (or just left) a path that re-detected it, don't double-handle.
        if (
            not ev.get("from_queue")
            and queued
            and queued.get("event") == ev_label
            and in_quiet_hours(now)
        ):
            log_wake_decision(
                "skipped",
                ev_label,
                ev["detail"],
                "already queued for 07:00 America/Edmonton",
                ev.get("time_utc"),
            )
            continue

        limit = rate_limit_reason(ev_label, state, now)
        if limit:
            log_wake_decision("skipped", ev_label, ev["detail"], limit, ev.get("time_utc"))
            if ev.get("from_queue"):
                # Leave it queued if quiet hours ended but a 24 h cap still
                # blocks delivery; drop it if it has gone stale.
                qtime = parse_iso(queued.get("time_utc") if queued else None)
                if qtime and now - qtime > timedelta(hours=24):
                    log_wake_decision(
                        "dropped",
                        ev_label,
                        ev["detail"],
                        "queued wake older than 24 h; dropping",
                        ev.get("time_utc"),
                    )
                    state["queue"] = None
                    queued = None
            continue

        if sent_this_cycle:
            log_wake_decision(
                "skipped",
                ev_label,
                ev["detail"],
                "another wake already handled this cycle (minimum 2 h between wakes)",
                ev.get("time_utc"),
            )
            continue

        if in_quiet_hours(now) and not ev.get("from_queue"):
            seven = next_seven_am(now).astimezone(EDMONTON).strftime("%Y-%m-%d %H:%M America/Edmonton")
            existing = state.get("queue")
            if existing:
                old_p = WAKE_PRIORITY.get(existing.get("event"), 99)
                new_p = ev["priority"]
                if new_p < old_p:
                    log_wake_decision(
                        "queued",
                        ev_label,
                        ev["detail"],
                        f"quiet hours 22:00–07:00 America/Edmonton; replaced queued {existing.get('event')} with higher-priority {ev_label}; will flush at {seven}",
                        ev.get("time_utc"),
                    )
                    state["queue"] = {
                        "event": ev_label,
                        "detail": ev["detail"],
                        "time_utc": ev["time_utc"],
                        "detected_utc": iso(now),
                    }
                    queued = state["queue"]
                else:
                    log_wake_decision(
                        "skipped",
                        ev_label,
                        ev["detail"],
                        f"quiet hours; kept queued {existing.get('event')} (higher or equal priority)",
                        ev.get("time_utc"),
                    )
            else:
                state["queue"] = {
                    "event": ev_label,
                    "detail": ev["detail"],
                    "time_utc": ev["time_utc"],
                    "detected_utc": iso(now),
                }
                queued = state["queue"]
                log_wake_decision(
                    "queued",
                    ev_label,
                    ev["detail"],
                    f"quiet hours 22:00–07:00 America/Edmonton; queued most important wake for {seven}",
                    ev.get("time_utc"),
                )
            continue

        decision, reason = deliver_or_would_wake(ev, url, key, now)
        log_wake_decision(decision, ev_label, ev["detail"], reason, ev.get("time_utc"))
        if decision in ("sent", "would_wake"):
            record_sent(state, ev, delivered=(decision == "sent"), now=now)
            sent_this_cycle = True
            if ev.get("from_queue") or (queued and queued.get("event") == ev_label):
                state["queue"] = None
                queued = None
        elif decision == "failed" and ev.get("from_queue"):
            # Keep the queue; next cycle will retry. Do not count against caps.
            pass

    if not candidates:
        log.info(
            "wake check: no events this cycle (webhook config %s)",
            "present" if (url and key) else "missing -> would_wake only",
        )

    try:
        save_wake_state(state)
    except OSError as exc:
        log.error("could not write wake state: %s", exc)


def cycle(do_metar: bool) -> None:
    dsn = fetch_dsn()
    try:
        append_jsonl(DSN_JSONL, dsn)
    except OSError as exc:
        log.error("failed to write %s: %s", DSN_JSONL, exc)
    metar = None
    if do_metar:
        metar = fetch_metar()
        try:
            append_jsonl(METAR_JSONL, metar)
        except OSError as exc:
            log.error("failed to write %s: %s", METAR_JSONL, exc)
    try:
        write_summary()
    except Exception as exc:
        log.error("summary generation failed: %s\n%s", exc, traceback.format_exc())
    try:
        process_wakes(dsn, metar)
    except Exception as exc:
        log.error("wake hook failed: %s\n%s", exc, traceback.format_exc())


def trim_file(path: Path, cutoff: datetime, jsonl: bool) -> None:
    """Drop lines older than cutoff so the repo stays small."""
    if not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    kept = []
    for line in lines:
        if not line.strip():
            continue
        ts = None
        if jsonl:
            try:
                ts = parse_iso(json.loads(line).get("ts_utc"))
            except (json.JSONDecodeError, AttributeError):
                ts = None
        else:
            m = re.search(r"\((\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ)\)", line)
            ts = parse_iso(m.group(1)) if m else None
        if ts is None or ts >= cutoff:
            kept.append(line)
    if len(kept) != len([l for l in lines if l.strip()]):
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text("".join(l + "\n" for l in kept), encoding="utf-8")
        tmp.replace(path)
        log.info("trimmed %s to last %d days (%d lines kept)", path.name, RETENTION_DAYS, len(kept))


def trim_all() -> None:
    cutoff = utcnow() - timedelta(days=RETENTION_DAYS)
    for path in (DSN_JSONL, METAR_JSONL):
        trim_file(path, cutoff, jsonl=True)
    trim_file(WAKES_LOG, cutoff, jsonl=False)


def last_metar_time() -> Optional[datetime]:
    if not METAR_JSONL.exists():
        return None
    last = None
    try:
        with METAR_JSONL.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = parse_iso(rec.get("ts_utc"))
                if ts:
                    last = ts
    except OSError:
        return None
    return last


def main() -> int:
    parser = argparse.ArgumentParser(description="Voyager DSN + PHNL watcher")
    parser.add_argument("--once", action="store_true", help="run one cycle and exit")
    parser.add_argument("--dsn-interval", type=int, default=DSN_INTERVAL_S)
    parser.add_argument("--metar-interval", type=int, default=METAR_INTERVAL_S)
    parser.add_argument(
        "--force-metar",
        action="store_true",
        help="with --once, fetch METAR even if the last fetch was under --metar-interval ago",
    )
    args = parser.parse_args()

    DIR.mkdir(parents=True, exist_ok=True)
    setup_logging(to_stdout=args.once)
    if not args.once:
        acquire_lock_or_exit()
        signal.signal(signal.SIGTERM, handle_stop)
        signal.signal(signal.SIGINT, handle_stop)
        log.info("watcher started pid=%s", os.getpid())

    try:
        if args.once:
            # One-shot (scheduled CI) mode: METAR only if it's due. Allow 60 s
            # of slack so scheduler jitter doesn't skip a half-hour slot.
            prev = last_metar_time()
            due = (
                args.force_metar
                or prev is None
                or utcnow() - prev >= timedelta(seconds=max(0, args.metar_interval - 60))
            )
            log.info("one-shot cycle: METAR %s (last fetch %s)", "due" if due else "not due", iso(prev))
            cycle(do_metar=due)
            trim_all()
            return 0 if DSN_JSONL.exists() else 1

        # Always fetch both on the first cycle so a fresh start has a complete snapshot.
        cycle(do_metar=True)

        last_metar = utcnow()
        while not _stop:
            deadline = time.monotonic() + max(30, args.dsn_interval)
            while not _stop and time.monotonic() < deadline:
                time.sleep(min(5, deadline - time.monotonic()))
            if _stop:
                break
            need_metar = True
            prev = last_metar_time() or last_metar
            if utcnow() - prev < timedelta(seconds=args.metar_interval):
                need_metar = False
            cycle(do_metar=need_metar)
            if need_metar:
                last_metar = utcnow()
        log.info("watcher stopped")
        return 0
    finally:
        if not args.once:
            release_lock()


if __name__ == "__main__":
    sys.exit(main())
