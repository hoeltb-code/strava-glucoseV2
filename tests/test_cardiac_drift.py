from types import SimpleNamespace

from app.logic import compute_terrain_adjusted_cardiac_drift


def _points(duration_minutes=60, slope=0.0, early_hr=140, late_hr=147):
    rows = []
    distance = altitude = 0.0
    for elapsed in range(0, duration_minutes * 60 + 1, 10):
        speed_mps = 3.0 if slope < 3 else 1.0
        distance += speed_mps * 10
        altitude += speed_mps * 10 * slope / 100
        progress = elapsed / (duration_minutes * 60)
        hr = early_hr if progress < 0.55 else late_hr
        rows.append(SimpleNamespace(
            elapsed_time=elapsed,
            distance=distance,
            altitude=altitude,
            heartrate=hr,
            slope_percent=slope,
            grade=slope,
            vertical_speed_m_per_h=speed_mps * slope / 100 * 3600 if slope > 0 else None,
            moving=True,
        ))
    return rows


def test_flat_drift_compares_speed_to_heart_rate():
    result = compute_terrain_adjusted_cardiac_drift(_points())
    assert result["available"] is True
    assert 4.0 <= result["percent"] <= 6.0
    assert result["terrain_rows"][0]["key"] == "rolling"


def test_climb_drift_uses_vam_efficiency():
    result = compute_terrain_adjusted_cardiac_drift(_points(slope=12.0, early_hr=145, late_hr=153))
    assert result["available"] is True
    assert result["percent"] > 4
    assert result["terrain_rows"][0]["key"] == "steep_climb"


def test_activity_under_45_minutes_is_not_scored():
    result = compute_terrain_adjusted_cardiac_drift(_points(duration_minutes=44))
    assert result["available"] is False
    assert "minimum 45 minutes" in result["reason"]
