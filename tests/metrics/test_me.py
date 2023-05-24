import datetime

from nowcasting_datamodel.models import MetricValueSQL
from nowcasting_metrics.metrics.me import (
    make_me,
    make_me_one_gsp_with_forecast_horizon_and_one_half_hour,
)


def test_make_me_forecast_horizon(db_session, gsp_yields, forecast_values, datetime_interval):
    results = make_me_one_gsp_with_forecast_horizon_and_one_half_hour(
        session=db_session,
        datetime_interval=datetime_interval,
        gsp_id=1,
        forecast_horizon_minutes=60,
    )

    value,n, time = results[0]

    assert value == 60  # (61 - 1)
    assert n == 1


def test_make_me(
    db_session, gsp_yields, forecast_values_latest, forecast_values, datetime_interval
):
    max_forecast_horizon_minutes = {"cnn": 240, "National_xg": 30 * 60, "pvnet_v2": 240}

    make_me(
        session=db_session,
        datetime_interval=datetime_interval,
        max_forecast_horizon_minutes=max_forecast_horizon_minutes,
    )

    # 3 models, 2 forecast horizon, 8 half hours
    assert db_session.query(MetricValueSQL).count() == 3 * 2 * 8
