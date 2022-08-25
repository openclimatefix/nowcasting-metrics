from nowcasting_datamodel.models.metric import Metric, DatetimeInterval, MetricValueSQL
from nowcasting_datamodel.models.gsp import LocationSQL
from nowcasting_datamodel.read.read_metric import get_metric, get_datetime_interval


def save_metric_value_to_database(
    session,
    value: float,
    number_of_data_points: int,
    metric: Metric,
    datetime_interval: DatetimeInterval,
    location: LocationSQL,
):
    """

    :param session:
    :param value:
    :param number_of_data_points:
    :param metric:
    :param datetime_interval:
    :return:
    """

    metric_sql = get_metric(session=session, name=metric.name)
    datetime_interval_sql = get_datetime_interval(
        session=session,
        start_datetime_utc=datetime_interval.start_datetime_utc,
        end_datetime_utc=datetime_interval.end_datetime_utc,
    )

    metric_value_sql = MetricValueSQL(
        value=value,
        number_of_data_points=number_of_data_points,
        metric=metric_sql,
        datetime_interval=datetime_interval_sql,
        location=location,
    )

    session.add(metric_value_sql)
