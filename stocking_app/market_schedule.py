"""
stocking_app/market_schedule.py
Market-hours logic — knows when each exchange is open.

Supports any exchange configured with `market_open` / `market_close` (HH:MM strings)
and an IANA timezone (e.g. "Asia/Kolkata", "Europe/London").

Public API
----------
is_market_open(tz, open_time, close_time) -> bool
next_event(tz, open_time, close_time)     -> (label: str, seconds: int)
market_status(tz, open_time, close_time)  -> dict   (rich info for heartbeat)
fmt_duration(seconds)                     -> str     ("2h 15m", "45m", "30s")
"""
from __future__ import annotations

from datetime import datetime, time as dtime


# ── Default market hours per ticker suffix ─────────────────────────────────────
_DEFAULTS: dict[str, tuple[str, str, str]] = {
    # suffix → (timezone, open_HH:MM, close_HH:MM)
    ".NS": ("Asia/Kolkata",   "09:15", "15:30"),
    ".BO": ("Asia/Kolkata",   "09:15", "15:30"),
    ".L":  ("Europe/London",  "08:00", "16:30"),
    ".AX": ("Australia/Sydney","10:00","16:10"),
    ".T":  ("Asia/Tokyo",     "09:00", "15:30"),
    ".HK": ("Asia/Hong_Kong", "09:30", "16:00"),
}

WEEKDAYS = {0, 1, 2, 3, 4}   # Mon–Fri


def _parse_hhmm(hhmm: str) -> dtime:
    h, m = hhmm.strip().split(":")
    return dtime(int(h), int(m))


def _now_local(tz: str) -> datetime:
    import zoneinfo
    try:
        zone = zoneinfo.ZoneInfo(tz)
    except Exception:  # noqa: BLE001  (bad tz string → fall back to UTC)
        import zoneinfo as zi
        zone = zi.ZoneInfo("UTC")
    return datetime.now(zone)


def is_market_open(tz: str, open_time: str, close_time: str) -> bool:
    """Return True if the market is currently open (weekday and within hours)."""
    now = _now_local(tz)
    if now.weekday() not in WEEKDAYS:
        return False
    t = now.time().replace(second=0, microsecond=0)
    return _parse_hhmm(open_time) <= t < _parse_hhmm(close_time)


def next_event(tz: str, open_time: str, close_time: str) -> tuple[str, int]:
    """
    Returns (event_label, seconds_until_event).
    event_label is "opens" or "closes".
    """
    import zoneinfo
    from datetime import timedelta

    try:
        zone = zoneinfo.ZoneInfo(tz)
    except Exception:
        zone = zoneinfo.ZoneInfo("UTC")

    now = datetime.now(zone)
    t_open  = _parse_hhmm(open_time)
    t_close = _parse_hhmm(close_time)
    today   = now.date()
    weekday = now.weekday()

    def _dt(date, t: dtime) -> datetime:
        return datetime(date.year, date.month, date.day, t.hour, t.minute, tzinfo=zone)

    # Is market currently open?
    if weekday in WEEKDAYS and t_open <= now.time().replace(second=0, microsecond=0) < t_close:
        close_dt = _dt(today, t_close)
        return "closes", max(0, int((close_dt - now).total_seconds()))

    # Find next open (skip weekends)
    candidate = today
    days_ahead = 0
    while True:
        candidate_wd = candidate.weekday()
        if candidate_wd in WEEKDAYS:
            open_dt = _dt(candidate, t_open)
            if open_dt > now:
                return "opens", max(0, int((open_dt - now).total_seconds()))
        days_ahead += 1
        if days_ahead > 7:
            break
        from datetime import timedelta as td
        candidate = today + td(days=days_ahead)

    return "opens", 0


def fmt_duration(seconds: int) -> str:
    """Format seconds as human-readable duration, e.g. '2h 15m', '45m', '30s'."""
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s   = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m:02d}m"
    if m > 0:
        return f"{m}m"
    return f"{s}s"


def defaults_for_suffix(suffix: str) -> tuple[str, str, str]:
    """Return (timezone, market_open, market_close) defaults for a ticker suffix."""
    return _DEFAULTS.get(suffix, ("UTC", "00:00", "23:59"))


def market_status(tz: str, open_time: str, close_time: str) -> dict:
    """
    Return a rich status dictionary for use in the engine heartbeat and UI.

    Keys:
      market_open   : bool
      market_tz     : str
      market_hours  : "09:15–15:30"
      next_event    : "opens" | "closes"
      next_event_in : "2h 15m"
      next_event_secs: int
    """
    open_flag = is_market_open(tz, open_time, close_time)
    evt_label, evt_secs = next_event(tz, open_time, close_time)
    return {
        "market_open":     open_flag,
        "market_tz":       tz,
        "market_hours":    f"{open_time}–{close_time}",
        "next_event":      evt_label,
        "next_event_in":   fmt_duration(evt_secs),
        "next_event_secs": evt_secs,
    }
