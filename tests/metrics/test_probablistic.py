from nowcasting_metrics.metrics.probablistic import (
make_probabilistic_metrics_one_forecast_horizon_minutes
)

from nowcasting_metrics.database.gsp_yield import get_gsp_yield
from nowcasting_metrics.database.forecast import get_forecast_values

from freezegun import freeze_time

@freeze_time("2022-01-01 00:00:00")
def test_make_prob_one_forecast_horizon(
    db_session, gsp_yields, forecast_values, datetime_interval
):
    db_session.commit()
    forecast_values = get_forecast_values(session=db_session, model_name="pvnet_v2" )
    gsp_yields_df = get_gsp_yield(session=db_session, gsp_id=1)

    value, pinball, n = make_probabilistic_metrics_one_forecast_horizon_minutes(
        session=db_session,
        datetime_interval=datetime_interval,
        model_name="cnn",
        forecast_horizon_minutes=0,
        p_level='10',
        forecast_values=forecast_values,
        gsp_yields=gsp_yields_df
    )

    assert value == 0.5  # half is the dummy data prediction plevel 10 larger than the truth
    assert pinball == ((3.6-1)*0.1 + (1-0.9)*0.9)/2 # which is 0.175
    assert n == 2

    value, pinball, n = make_probabilistic_metrics_one_forecast_horizon_minutes(
        session=db_session,
        datetime_interval=datetime_interval,
        model_name="cnn",
        forecast_horizon_minutes=0,
        p_level='90',
        forecast_values = forecast_values,
        gsp_yields = gsp_yields_df
    )

    assert value == 1.0  # all is the dummy data prediction plevel 90 larger than the truth
    assert pinball == ((4.4-1)*0.9 + (1.1-1)*0.9)/2
    assert n == 2


