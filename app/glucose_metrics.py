"""Shared time-weighted CGM metrics; missing intervals never count as observations."""
import datetime as dt
import math
import statistics

LOW, HIGH = 70., 180.
VERSION = "duration-v2"


def timestamp(value):
    if isinstance(value, dt.datetime):
        return (value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)).timestamp()
    if isinstance(value, str):
        try:
            return timestamp(dt.datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            return None
    if isinstance(value, (float, int)) and math.isfinite(value):
        return float(value)
    return None


def zone_index(value):
    return 0 if value < LOW else 1 if value < 100 else 2 if value < 140 else 3 if value <= HIGH else 4


def summarize(samples, start=None, end=None, max_gap_seconds=None):
    unique = {}
    for sample in samples or []:
        t = timestamp(sample.get("ts"))
        value = sample.get("mgdl")
        if t is not None and isinstance(value, (int, float)) and math.isfinite(value) and value > 0:
            unique[t] = float(value)
    points = sorted(unique.items())
    lower, upper = timestamp(start), timestamp(end)
    if lower is None and points:
        lower = points[0][0]
    if upper is None and points:
        upper = points[-1][0]
    window = max(0., upper - lower) if lower is not None and upper is not None else 0.
    gaps = [b[0]-a[0] for a,b in zip(points, points[1:]) if b[0] > a[0]]
    # Policy, not a medical threshold: 1.5 × typical sampling period, bounded.
    max_gap = max_gap_seconds if max_gap_seconds is not None else max(90., min(900., statistics.median(gaps)*1.5 if gaps else 90.))
    intervals = []
    seconds = [0.] * 5
    weighted = squared = covered = 0.
    for (t0, value), (t1, _) in zip(points, points[1:]):
        if t1-t0 > max_gap:
            continue
        a, b = max(t0, lower), min(t1, upper)
        if b <= a:
            continue
        duration = b-a
        seconds[zone_index(value)] += duration
        weighted += value * duration
        squared += value**2 * duration
        covered += duration
        intervals.append({"start": a, "end": b, "mgdl": value, "zone": zone_index(value)})
    values = [(t,v) for t,v in points if lower <= t <= upper] if points and lower is not None and upper is not None else []
    average = weighted / covered if covered else None
    variance = max(0., squared/covered-average**2) if covered else None
    return {"available": covered > 0, "avg": average,
            "min": min((v for _,v in values), default=None), "max": max((v for _,v in values), default=None),
            "start_mgdl": values[0][1] if values else None, "end_mgdl": values[-1][1] if values else None,
            "observed_seconds": covered, "missing_seconds": max(0., window-covered), "duration_seconds": window,
            "coverage_percent": covered/window*100 if window else None,
            "pct_in_range": sum(seconds[1:4])/covered*100 if covered else None,
            "pct_hypo": seconds[0]/covered*100 if covered else None,
            "pct_hyper": seconds[4]/covered*100 if covered else None,
            "low_seconds": seconds[0], "high_seconds": seconds[4], "zone_seconds": seconds,
            "variability_pct": math.sqrt(variance)/average*100 if average else None,
            "n": len(values), "max_gap_seconds": max_gap, "intervals": intervals, "version": VERSION}


def chart_series(samples, start=None, end=None):
    metrics = summarize(samples, start, end)
    points = sorted((timestamp(p.get("ts")), p.get("mgdl")) for p in samples
                    if timestamp(p.get("ts")) is not None and isinstance(p.get("mgdl"), (int,float)) and math.isfinite(p["mgdl"]))
    result = []
    previous = None
    lower, upper = timestamp(start), timestamp(end)
    for t,v in points:
        if (lower is not None and t < lower) or (upper is not None and t > upper):
            continue
        if previous is not None and t-previous > metrics["max_gap_seconds"]:
            result.append({"x": (previous+t)/2, "y": None})
        result.append({"x": t, "y": v})
        previous=t
    return result
