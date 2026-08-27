from app.logic import compute_vertical_speed_series


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
