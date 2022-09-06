from click.testing import CliRunner
from nowcasting_datamodel.models.metric import MetricValueSQL, MetricSQL
from nowcasting_datamodel.models.gsp import GSPYieldSQL
from nowcasting_datamodel.models.models import ForecastValueLatestSQL

from nowcasting_metrics.app import app


def test_app(db_connection, db_session, gsp_yields, forecast_values_latest, forecast_values):

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
    assert len(metric_values) == 110
    # National + 5*GSPs + All GSPS = 7
    # 8 forecast horizones * (National + 5 GSPS) = 48
    # Total metrics is 55
    # x2 due to MAE and RMSE

    metrics = db_session.query(MetricSQL).all()
    assert len(metrics) == 4
