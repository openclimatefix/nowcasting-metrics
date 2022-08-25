from nowcasting_metrics.metrics.metrics import get_metrics
from nowcasting_datamodel.models.metric import MetricSQL



def test_get_metrics(db_session):
    metrics = get_metrics(session=db_session)

    assert len(metrics) == 2
    assert len(db_session.query(MetricSQL).all()) == 2


def test_get_metrics_twice(db_session):
    _ = get_metrics(session=db_session)
    metrics = get_metrics(session=db_session)

    assert len(metrics) == 2
    assert len(db_session.query(MetricSQL).all()) == 2