from nowcasting_metrics.metrics.probablistic import (
make_probabilistic_metrics_one_forecast_horizon_minutes
)


def test_make_ramp_rate_one_forecast_horizon(
    db_session, gsp_yields, forecast_values, datetime_interval
):
    value, pinball, n = make_probabilistic_metrics_one_forecast_horizon_minutes(
        session=db_session,
        datetime_interval=datetime_interval,
        model_name="cnn",
        forecast_horizon_minutes=0,
        p_level='10'
    )

    assert value == 0.5  # half is the dummy data prediction plevel 10 larger than the truth
    assert pinball == ((3.6-1)*0.1 + (1-0.9)*0.9)/2
    assert n == 2

    value, pinball, n = make_probabilistic_metrics_one_forecast_horizon_minutes(
        session=db_session,
        datetime_interval=datetime_interval,
        model_name="cnn",
        forecast_horizon_minutes=0,
        p_level='90'
    )

    assert value == 1.0  # all is the dummy data prediction plevel 90 larger than the truth
    assert pinball == ((4.4-1)*0.9 + (1.1-1)*0.9)/2
    assert n == 2


