from nowcasting_datamodel.models.metric import MetricSQL

from nowcasting_metrics.metrics.metrics import check_metrics_in_database


def test_get_metrics(db_session):
    metrics = check_metrics_in_database(session=db_session)

    assert len(metrics) == 2 * 17  # 0,0.5, 1 to 8 hours for 2 mae and rmse
    assert (
        len(db_session.query(MetricSQL).all()) == 2 * 17
    )  # 0,0.5, 1 to 8 hours for 2 mae and rmse


def test_get_metrics_twice(db_session):
    _ = check_metrics_in_database(session=db_session)
    metrics = check_metrics_in_database(session=db_session)

    assert len(metrics) == 2 * 17  # 0,0.5, 1 to 8 hours for 2 mae and rmse
    assert (
        len(db_session.query(MetricSQL).all()) == 2 * 17
    )  # 0,0.5, 1 to 8 hours for 2 mae and rmse
