from nowcasting_metrics.metrics.ramp_rate import (
    make_ramp_rate_one_forecast_horizon_minutes,
    make_ramp_rate,
)
from nowcasting_metrics.database.gsp_yield import get_gsp_yield
from nowcasting_metrics.database.forecast import get_all_forecast_values, get_forecast_values

from freezegun import freeze_time

from nowcasting_datamodel.models import ForecastSQL, ForecastValueLatestSQL, ForecastValueSevenDaysSQL, MLModelSQL

@freeze_time("2022-01-01")
def test_make_ramp_rate_one_forecast_horizon(
    db_session, gsp_yields, forecast_values_same_creation, datetime_interval
):
    db_session.commit()
    forecast_values = get_forecast_values(session=db_session, model_name="pvnet_v2")
    gsp_yields_df = get_gsp_yield(session=db_session, gsp_id=0)

    value, n = make_ramp_rate_one_forecast_horizon_minutes(
        session=db_session,
        datetime_interval=datetime_interval,
        model_name="pvnet_v2",
        forecast_horizon_minutes=0,
        ramp_rate_minutes=30,
        forecast_values=forecast_values,
        gsp_yields=gsp_yields_df,
    )

    assert value == 3  # (3-1)-(1-1)
    assert n == 1


@freeze_time("2022-01-01")
def test_make_ramp_rate(db_session, gsp_yields, forecast_values_same_creation, datetime_interval):

    forecast_values = get_all_forecast_values(session=db_session)
    gsp_yields_df = get_gsp_yield(session=db_session, gsp_id=0)

    make_ramp_rate(
        session=db_session,
        datetime_interval=datetime_interval,
        all_forecast_values=forecast_values,
        gsp_yields=gsp_yields_df,
    )
