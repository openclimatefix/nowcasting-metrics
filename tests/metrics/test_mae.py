from nowcasting_metrics.metrics.mae import (
    make_mae_one_gsp,
    make_mae,
    make_mae_one_gsp_forecast_horizon,
)


def test_make_mae(db_session, gsp_yields, forecast_values_latest, datetime_interval):

    value, n = make_mae_one_gsp(session=db_session, datetime_interval=datetime_interval, gsp_id=1)

    assert value == 1.5  # (1-1)*0.5 + (4-1)*0.5
    assert n == 2


def test_make_mae_all_gsp(
    db_session, gsp_yields, forecast_values_latest, datetime_interval, forecast_values
):

    make_mae(session=db_session, datetime_interval=datetime_interval, n_gsps=5)


def test_make_mae_one_gsp_forecast_horizon(
    db_session, gsp_yields, forecast_values, datetime_interval
):

    value, n = make_mae_one_gsp_forecast_horizon(
        session=db_session,
        datetime_interval=datetime_interval,
        gsp_id=1,
        forecast_horizon_minutes=60,
    )

    assert n == 2
    assert value == 2.5  # (1+1-1)*0.5 + (4+1-1)*0.5 =
