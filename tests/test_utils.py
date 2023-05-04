from nowcasting_metrics.utils import save_metric_value_to_database
from nowcasting_datamodel.models import MetricSQL


def test_save_metric_value_to_database_location_none(db_session, datetime_interval):
    """Save one metric value=None to the database with location=None"""

    metric = MetricSQL(name="test_model", description="test model")

    save_metric_value_to_database(
        session=db_session,
        value=None,
        number_of_data_points=0,
        metric=metric,
        datetime_interval=datetime_interval,
    )
