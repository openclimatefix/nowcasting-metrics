from nowcasting_metrics.metrics.ramp_rate import (
    make_ramp_rate_one_forecast_horizon_minutes,
    make_ramp_rate,
)


def test_make_ramp_rate_one_forecast_horizon(
    db_session, gsp_yields, forecast_values_same_creation, datetime_interval
):
    value, n = make_ramp_rate_one_forecast_horizon_minutes(
        session=db_session,
        datetime_interval=datetime_interval,
        model_name="cnn",
        forecast_horizon_minutes=0,
        ramp_rate_minutes=30,
    )

    assert value == 3  # (3-1)-(1-1)
    assert n == 1


def test_make_ramp_rate(db_session, gsp_yields, forecast_values_same_creation, datetime_interval):
    make_ramp_rate(
        session=db_session,
        datetime_interval=datetime_interval,
    )
