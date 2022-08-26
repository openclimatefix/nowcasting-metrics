from click.testing import CliRunner
from nowcasting_datamodel.models.gsp import GSPYieldSQL
from nowcasting_datamodel.models.metric import MetricValueSQL, MetricSQL
from nowcasting_datamodel.models.models import ForecastValueLatestSQL

from nowcasting_metrics.app import app


def test_app(db_connection, db_session, gsp_yields, forecast_values_latest):

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
    assert len(metric_values) == 6 * 2

    metrics = db_session.query(MetricSQL).all()
    assert len(metrics) == 2
