from nowcasting_datamodel.models.metric import Metric, DatetimeInterval, MetricValue
from nowcasting_datamodel.read.read_metric import get_metric, get_datetime_interval


def save_metric_value_to_database(
    session,
    value: float,
    number_of_data_points: int,
    metric: Metric,
    datetime_interval: DatetimeInterval,
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

    metric_value = MetricValue(value=value, number_of_data_points=number_of_data_points)

    metric_value_sql = metric_value.to_orm()
    metric_value_sql.metric = metric_sql
    metric_value_sql.datetime_interval = datetime_interval_sql

    session.add(metric_value_sql)
