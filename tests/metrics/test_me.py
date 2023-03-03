import datetime

from nowcasting_metrics.metrics.me import (
    make_me,
    make_me_one_gsp_with_forecast_horizon_and_one_half_hour,
)


def test_make_me_forecast_horizon(db_session, gsp_yields, forecast_values, datetime_interval):
    value, n = make_me_one_gsp_with_forecast_horizon_and_one_half_hour(
        session=db_session,
        datetime_interval=datetime_interval,
        gsp_id=1,
        forecast_horizon_minutes=60,
        time_of_day=datetime.time(0, 30, 0),
    )

    assert value == 60  # (61 - 1)
    assert n == 1


def test_make_mae_five_gsp(
    db_session, gsp_yields, forecast_values_latest, forecast_values, datetime_interval
):
    make_me(
        session=db_session,
        datetime_interval=datetime_interval,
        max_forecast_horizon_minutes=240,
    )
