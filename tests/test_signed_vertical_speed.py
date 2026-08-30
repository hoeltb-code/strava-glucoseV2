from types import SimpleNamespace

from app.logic import compute_vertical_speed_series, is_valid_activity_stream_interval


def test_vertical_speed_keeps_descent_sign():
    values = compute_vertical_speed_series(
        elapsed_stream=[0, 10, 20, 30, 40],
        alt_stream=[100, 98, 96, 94, 92],
        window_pts=1,
        min_dt=10,
        only_ascent=False,
    )
    assert all(value is not None and value < 0 for value in values)
    assert round(values[2]) == -720


def test_ascent_only_mode_still_excludes_descents():
    values = compute_vertical_speed_series(
        elapsed_stream=[0, 10, 20],
        alt_stream=[100, 98, 96],
        window_pts=1,
        min_dt=10,
        only_ascent=True,
    )
    assert values == [None, None, None]


def _point(time, distance, altitude, moving=True):
    return SimpleNamespace(
        elapsed_time=time,
        distance=distance,
        altitude=altitude,
        moving=moving,
    )


def test_stream_interval_rejects_pause_and_resume_gap():
    assert not is_valid_activity_stream_interval(
        _point(120, 500, 1100),
        _point(720, 520, 1650),
    )


def test_stream_interval_rejects_non_moving_points():
    assert not is_valid_activity_stream_interval(
        _point(120, 500, 1100, moving=False),
        _point(121, 503, 1101),
    )


def test_stream_interval_keeps_plausible_running_ascent():
    assert is_valid_activity_stream_interval(
        _point(120, 500, 1100),
        _point(125, 515, 1104),
    )
