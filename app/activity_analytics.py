"""Small persisted read model, rebuilt at import/maintenance instead of each GET."""
import datetime as dt
import math
from .glucose_metrics import summarize, chart_series
from .energy import activity_energy

VERSION = 2


def downsample(points, limit=400, fields=("y",)):
    """Bucket extrema preserve peaks and gaps; endpoints are always retained."""
    if len(points) <= limit:
        return points
    keep = {0, len(points)-1}
    budget = max(1, (limit-2)//(2*len(fields)+2))
    width = math.ceil(len(points)/budget)
    for start in range(0, len(points), width):
        indices = range(start, min(len(points),start+width))
        keep.add(start)
        for field in fields:
            valid = [i for i in indices if isinstance(points[i].get(field),(int,float)) and math.isfinite(points[i][field])]
            if valid:
                keep.update((min(valid,key=lambda i:points[i][field]),max(valid,key=lambda i:points[i][field])))
        missing = [i for i in indices if points[i].get(fields[0]) is None]
        if missing:
            keep.add(missing[0])
    selected=[]
    previous=None
    for i in sorted(keep):
        if previous is not None and i>previous+1:
            missing=next((p for p in points[previous+1:i] if any(p.get(f) is None for f in fields)),None)
            if missing is not None:
                selected.append({**missing,**{f:None for f in fields}})
        selected.append(points[i]);previous=i
    return selected


def build_summary(activity, user, points, samples=None):
    start = activity.start_date
    if start.tzinfo is None:
        start = start.replace(tzinfo=dt.timezone.utc)
    end = start + dt.timedelta(seconds=activity.elapsed_time or 0)
    source="sensor" if samples is not None else "activity_stream"
    if samples is None:
        samples = [{"ts": start+dt.timedelta(seconds=p.elapsed_time), "mgdl":p.glucose_mgdl}
                   for p in points if p.elapsed_time is not None and p.glucose_mgdl is not None]
    glucose = summarize(samples,start,end)
    glucose["source"] = source
    glucose.pop("intervals", None)
    glucose_chart = [{"x":(p["x"]-start.timestamp())/60,"y":p["y"]} for p in chart_series(samples,start,end)]
    effort = []
    gain = loss = 0.
    previous_alt = None
    for p in points:
        if p.altitude is not None and previous_alt is not None:
            delta=p.altitude-previous_alt
            gain+=max(delta,0)
            loss+=max(-delta,0)
        if p.altitude is not None:
            previous_alt=p.altitude
        if p.elapsed_time is not None:
            effort.append({"x":p.elapsed_time/60,"y":p.heartrate,"altitude":p.altitude,
                           "pace":1000/p.velocity/60 if p.velocity and p.velocity>0 else None})
    energy = activity_energy(points,user.weight_kg,sport=activity.sport or "unknown",max_hr=user.max_heartrate,duration_seconds=activity.elapsed_time)
    return {"version":VERSION,"glycemia":glucose,"glucose_chart":downsample(glucose_chart),
            "effort_chart":downsample(effort,fields=("y","altitude","pace")),
            "energy":energy,"elevation_gain_m":gain,"elevation_loss_m":loss,
            "generated_at":dt.datetime.now(dt.timezone.utc).isoformat()}


def sparkline(series, *, glucose=False, width=320, height=72):
    points=downsample(series,90)
    valid=[p for p in points if isinstance(p.get("y"),(int,float)) and math.isfinite(p["y"])]
    if len(valid)<2:
        return None
    xmin,xmax=points[0]["x"],points[-1]["x"]
    low=min(40.,min(p["y"] for p in valid)) if glucose else min(p["y"] for p in valid)
    high=max(250.,max(p["y"] for p in valid)) if glucose else max(p["y"] for p in valid)
    x=lambda t:3+(t-xmin)/max(xmax-xmin,1)*(width-6)
    y=lambda v:3+(high-v)/max(high-low,1)*(height-6)
    path=[]; connected=False
    for p in points:
        if p.get("y") is None:
            connected=False
            continue
        path.append(f"{'L' if connected else 'M'}{x(p['x']):.1f},{y(p['y']):.1f}")
        connected=True
    line=" ".join(path)
    return {"line":line,"area":line+f" L{width-3},{height} L3,{height} Z", "width":width,"height":height,
            "range_top":y(180) if glucose else None,"range_height":y(70)-y(180) if glucose else None}
