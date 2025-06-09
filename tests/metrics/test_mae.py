from nowcasting_metrics.metrics.mae import (
    make_mae,
    make_mae_all_gsp,
    make_mae_one_gsp,
    make_mae_values,
    make_pvlive_mae,
)
from nowcasting_metrics.database.gsp_yield import get_gsp_yield
from nowcasting_metrics.database.forecast import get_forecast_values, get_all_forecast_values

from freezegun import freeze_time


def test_make_mae(db_session, gsp_yields, forecast_values_latest, datetime_interval):
    value, value_adjuster, n = make_mae_one_gsp(session=db_session, datetime_interval=datetime_interval, gsp_id=1, model_name="cnn")

    assert value == 1.5  # (1-1)*0.5 + (4-1)*0.5
    assert n == 2


@freeze_time("2022-01-01")
def test_make_mae_forecast_horizon(db_session, gsp_yields, forecast_values, datetime_interval):

    db_session.commit()
    forecast_values = get_all_forecast_values(session=db_session)
    gsp_yields_df = get_gsp_yield(session=db_session, gsp_id=1)

    forecast_values = forecast_values['pvnet_v2']
    assert len(forecast_values) >0
    assert len(gsp_yields_df) > 0

    value, value_adjuster, n = make_mae_values(
        session=db_session,
        datetime_interval=datetime_interval,
        gsp_yields=gsp_yields_df,
        forecast_values=forecast_values,
        forecast_horizon_minutes=60,
    )

    assert value == 1.5 + 60  # (61-1)*0.5 + (64-1)*0.5
    assert n == 2


def test_make_mae_all_gsp(
    db_session, gsp_yields, forecast_values_latest, forecast_values, datetime_interval
):
    value, n = make_mae_all_gsp(session=db_session, datetime_interval=datetime_interval, model_name="cnn")

    assert value == 1.5  # (1-1)*0.5 + (4-1)*0.5
    assert n == 10  # 2 *5


def test_make_mae_five_gsp(
    db_session, gsp_yields, forecast_values_latest, forecast_values, datetime_interval
):
    forecast_values = get_all_forecast_values(session=db_session)
    gsp_yields_df = get_gsp_yield(session=db_session, gsp_id=0)

    make_mae(
        session=db_session,
        datetime_interval=datetime_interval,
        n_gsps=5,
        max_forecast_horizon_minutes={"cnn": 240, "National_xg": 240, "pvnet_v2": 240},
        all_forecast_values=forecast_values,
        gsp_yields=gsp_yields_df
    )


def test_make_pvlive_mae(db_session, gsp_yields, gsp_yields_inday, datetime_interval):
    value, n = make_pvlive_mae(session=db_session, datetime_interval=datetime_interval, gsp_id=0)

    assert n == 2
    assert value == 1.0  # (2-1)*0.5 + (2-1)*0.5
