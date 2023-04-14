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
    max_forecast_horizon_minutes = {"cnn": 240, "National_xg": 30 * 60}

    make_me(
        session=db_session,
        datetime_interval=datetime_interval,
        max_forecast_horizon_minutes=max_forecast_horizon_minutes,
    )

    # 2 models, 2 forecast horizon, 8 half hours
    metric_values = db_session.query(MetricValueSQL).all()
    for m in metric_values:
        print(m.forecast_horizon_minutes, m.time_of_day, m.value, m.number_of_data_points)
    assert db_session.query(MetricValueSQL).count() == 2 * 2 * 8
