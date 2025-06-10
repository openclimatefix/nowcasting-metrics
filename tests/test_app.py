from click.testing import CliRunner
from nowcasting_datamodel.models import ForecastValueLatestSQL
from nowcasting_datamodel.models.gsp import GSPYieldSQL
from nowcasting_datamodel.models.metric import MetricSQL, MetricValueSQL

from nowcasting_metrics.metrics.me import me_hh
from nowcasting_metrics.metrics.mae import latest_mae
from nowcasting_metrics.app import app

from freezegun import freeze_time

@freeze_time("2022-01-01 00:00:00")
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
    if not response.exit_code == 0:
        raise response.exception

    # check me results
    # 2 models, 2 forecast horizon, 8 half hours
    metric_values = (
        db_session.query(MetricValueSQL).join(MetricSQL).filter(MetricSQL.name == me_hh.name).all()
    )
    assert len(metric_values) == 32

    # check mae results
    # 2 models with None + 8 forecast horizons = 18
    # 1 models at gsp level from 1-5 = 10
    # total is 23
    metric_values = (
        db_session.query(MetricValueSQL).join(MetricSQL).filter(MetricSQL.name == latest_mae.name).all()
    )

    assert len(metric_values) == 23

    # check all metrics
    metric_values = db_session.query(MetricValueSQL).all()
    assert len(metric_values) == 144
    # National
    # - with and without adjuster = 2
    # - 8 forecast horizons with and without adjuster = 16
    # - 2 models
    # - Total is 18*3 = 36
    # GSP
    # - 5*GSPs + All GSPS = 6
    # GPS PVlive = 6
    # Total metrics 48
    # RMSE has been removed
    # + ME # 2 models * 8 forecast horizons * 2 half hours  = 32
    # + Ramp rate 2 models * 3 forecast horizons  = 6 # TODO, not working
    # Total is 80
    # Pinball 2 models * 8 forecast horizons* 2 p levels  = 32
    # Exceedance 2 models * 8 forecast horizons * 2 p levels  = 32
    # Total 144

    metrics = db_session.query(MetricSQL).all()
    assert len(metrics) == 12
