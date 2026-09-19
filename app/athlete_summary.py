"""Read-only summaries from persisted activity analytics, with matched periods."""
import datetime as dt
from .models import Activity, RunnerProfileActivityContribution
from types import SimpleNamespace
from .logic import sport_column_condition
from .activity_analytics import sparkline


def aggregate(rows):
    result = dict(count=len(rows), covered_count=0, pending=0, distance_km=0., seconds=0., observed_seconds=0., low_seconds=0., high_seconds=0., in_range_seconds=0., kcal=0., energy_count=0)
    weeks = {}
    for row in rows:
        result['distance_km'] += (row.distance or 0)/1000
        result['seconds'] += row.elapsed_time or 0
        summary=row.analytics_summary or {}
        result['pending'] += not bool(summary)
        glucose=summary.get('glycemia') or {}
        observed=glucose.get('observed_seconds') or 0
        if glucose.get('version') != 'duration-v2':
            observed=0
        result['covered_count'] += observed > 0
        result['observed_seconds'] += observed
        result['low_seconds'] += glucose.get('low_seconds',0) if observed else 0
        result['high_seconds'] += glucose.get('high_seconds',0) if observed else 0
        result['in_range_seconds'] += observed * (glucose.get('pct_in_range') or 0)/100
        energy=summary.get('energy') or {}
        if energy.get('available'):
            result['kcal'] += energy.get('kcal') or 0
            result['energy_count'] += 1
        monday=(row.start_date-dt.timedelta(days=row.start_date.weekday())).date().isoformat()
        bucket=weeks.setdefault(monday,dict(date=monday,observed=0.,in_range=0.,count=0))
        bucket['observed'] += observed
        bucket['in_range'] += observed*(glucose.get('pct_in_range') or 0)/100
        bucket['count'] += observed > 0
    result['tir']=100*result['in_range_seconds']/result['observed_seconds'] if result['observed_seconds'] else None
    result['coverage']=100*result['observed_seconds']/result['seconds'] if result['seconds'] else None
    result['weeks']=[dict(v,tir=100*v['in_range']/v['observed'] if v['observed'] else None) for _,v in sorted(weeks.items())]
    return result


def load_summary(db,user_id,sport='all',start=None,end=None):
    end=end or dt.datetime.utcnow()
    def rows_for(lower,upper):
        query=db.query(Activity.id,Activity.strava_activity_id,Activity.name,Activity.start_date,Activity.sport,Activity.distance,Activity.elapsed_time,Activity.analytics_summary).filter(Activity.user_id==user_id,Activity.start_date<upper)
        archive=db.query(RunnerProfileActivityContribution).filter(RunnerProfileActivityContribution.user_id==user_id,RunnerProfileActivityContribution.metric_scope=="activity_meta",RunnerProfileActivityContribution.activity_start_date<upper)
        if sport!='all':
            query=query.filter(sport_column_condition(Activity.sport,sport))
            archive=archive.filter(sport_column_condition(RunnerProfileActivityContribution.sport,sport))
        if lower:
            query=query.filter(Activity.start_date>=lower)
            archive=archive.filter(RunnerProfileActivityContribution.activity_start_date>=lower)
        rows=query.order_by(Activity.start_date.desc()).all()
        live_ids={row.strava_activity_id for row in rows}
        for row in archive.all():
            if row.strava_activity_id in live_ids:continue
            summary=(row.extra or {}).get('analytics_summary') or {'archived':True}
            rows.append(SimpleNamespace(id=None,name=row.activity_name,start_date=row.activity_start_date,sport=row.sport,distance=row.total_distance_m,elapsed_time=row.total_duration_sec,analytics_summary=summary))
        return sorted(rows,key=lambda row:row.start_date,reverse=True)
    rows=rows_for(start,end)
    current=aggregate(rows)
    previous=aggregate(rows_for(start-(end-start),start)) if start else None
    current['previous']=previous
    current['delta_tir']=current['tir']-previous['tir'] if previous and previous['tir'] is not None and current['tir'] is not None else None
    current['latest']=None
    last=next((row for row in rows if row.id is not None),None)
    if last:
        current['latest']={'id':last.id,'name':last.name or 'Activité','date':last.start_date.strftime('%d/%m/%Y'),'chart':sparkline((last.analytics_summary or {}).get('glucose_chart') or [],glucose=True)}
    return current
