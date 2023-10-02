from click.testing import CliRunner
from nowcasting_datamodel.models import ForecastValueLatestSQL
from nowcasting_datamodel.models.gsp import GSPYieldSQL
from nowcasting_datamodel.models.metric import MetricSQL, MetricValueSQL

from nowcasting_metrics.app import app


def test_app(
    db_connection,
    db_session,
    gsp_yields,
    gsp_yields_inday,
    forecast_values_latest,
    forecast_values,
):
    assert (
        len(
            db_session.query(GSPYieldSQL)
            .filter(GSPYieldSQL.datetime_utc > "2022-01-01")
            .filter(GSPYieldSQL.datetime_utc < "2022-01-02")
            .all()
        )
        > 0
    )
    assert (
        len(
            db_session.query(ForecastValueLatestSQL)
            .filter(ForecastValueLatestSQL.target_time > "2022-01-01")
            .filter(ForecastValueLatestSQL.target_time < "2022-01-02")
            .all()
        )
        > 0
    )
    db_session.commit()

    runner = CliRunner()
    response = runner.invoke(
        app, ["--db-url", db_connection.url, "--n-gsps", 5, "--datetime-now", "2022-01-02"]
    )
    assert response.exit_code == 0, response.exception

    metric_values = db_session.query(MetricValueSQL).all()
    assert len(metric_values) == 256
    # National
    # - with and without adjuster = 2
    # - 8 forecast horizons with and without adjuster = 16
    # - 3 models
    # - Total is 18*3 = 54
    # GSP
    # - 5*GSPs + All GSPS = 6
    # - x2 models = 12
    # GPS PVlive = 6
    # Total metrics 72
    # x2 due to MAE and RMSE
    # Total is 144
    # + ME # 3 models * 8 forecast horizons * 2 half hours  = 48
    # + Ramp rate 3 models * 3 forecast horizons  = 9 # TODO this is 0 right now
    # Total is 192
    # Pinball 2 models * 8 forecast horizons* 2 p levels  = 32
    # Exceedance 2 models * 8 forecast horizons * 2 p levels  = 32
    # Total 256

    metrics = db_session.query(MetricSQL).all()
    assert len(metrics) == 12
