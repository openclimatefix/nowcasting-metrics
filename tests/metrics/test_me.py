from nowcasting_datamodel.models import MetricValueSQL
from nowcasting_metrics.metrics.me import (
    make_me,
    make_me_one_gsp_with_forecast_horizon_and_one_half_hour,
)
from nowcasting_metrics.database.gsp_yield import get_gsp_yield
from nowcasting_metrics.database.forecast import get_forecast_values, get_all_forecast_values

from freezegun import freeze_time


@freeze_time("2022-01-01 00:00:00")
def test_make_me_forecast_horizon(db_session, gsp_yields, forecast_values, datetime_interval):

    db_session.commit()
    forecast_values_df = get_forecast_values(session=db_session, model_name="pvnet_v2")
    gsp_yields_df = get_gsp_yield(session=db_session, gsp_id=1)

    results = make_me_one_gsp_with_forecast_horizon_and_one_half_hour(
        session=db_session,
        datetime_interval=datetime_interval,
        gsp_id=1,
        forecast_horizon_minutes=60,
        forecast_values=forecast_values_df,
        gsp_yields=gsp_yields_df,
    )
    value, n, time = results[0]

    assert value == 60  # (61 - 1)
    assert n == 1


@freeze_time("2022-01-01 00:00:00")
def test_make_me(
    db_session, gsp_yields, forecast_values_latest, forecast_values, datetime_interval
):
    max_forecast_horizon_minutes = {"National_xg": 30 * 60, "pvnet_v2": 240}

    db_session.commit()
    forecast_values = get_all_forecast_values(session=db_session)
    gsp_yields_df = get_gsp_yield(session=db_session, gsp_id=1)

    make_me(
        session=db_session,
        datetime_interval=datetime_interval,
        max_forecast_horizon_minutes=max_forecast_horizon_minutes,
        all_forecast_values=forecast_values,
        gsp_yields=gsp_yields_df,
    )

    # 3 models, 2 forecast horizon, 8 half hours
    assert db_session.query(MetricValueSQL).count() == 2 * 2 * 8
