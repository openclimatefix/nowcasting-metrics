from nowcasting_metrics.metrics.mae import (
    make_mae,
    make_mae_all_gsp,
    make_mae_one_gsp,
    make_mae_one_gsp_with_forecast_horizon,
    make_pvlive_mae,
)


def test_make_mae(db_session, gsp_yields, forecast_values_latest, datetime_interval):
    value, n = make_mae_one_gsp(session=db_session, datetime_interval=datetime_interval, gsp_id=1)

    assert value == 1.5  # (1-1)*0.5 + (4-1)*0.5
    assert n == 2


def test_make_mae_forecast_horizon(db_session, gsp_yields, forecast_values, datetime_interval):
    value, n = make_mae_one_gsp_with_forecast_horizon(
        session=db_session,
        datetime_interval=datetime_interval,
        gsp_id=1,
        forecast_horizon_minutes=60,
    )

    assert value == 1.5 + 60  # (61-1)*0.5 + (64-1)*0.5
    assert n == 2


def test_make_mae_all_gsp(
    db_session, gsp_yields, forecast_values_latest, forecast_values, datetime_interval
):
    value, n = make_mae_all_gsp(session=db_session, datetime_interval=datetime_interval)

    assert value == 1.5  # (1-1)*0.5 + (4-1)*0.5
    assert n == 10  # 2 *5


def test_make_mae_five_gsp(
    db_session, gsp_yields, forecast_values_latest, forecast_values, datetime_interval
):
    make_mae(
        session=db_session,
        datetime_interval=datetime_interval,
        n_gsps=5,
        max_forecast_horizon_minutes=240,
    )


def test_make_pvlive_mae(db_session, gsp_yields, gsp_yields_inday, datetime_interval):

    value, n = make_pvlive_mae(session=db_session, datetime_interval=datetime_interval, gsp_id=0)

    assert n == 2
    assert value == 1.0  # (2-1)*0.5 + (2-1)*0.5
