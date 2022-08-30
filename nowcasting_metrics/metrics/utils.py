""" util functions for metrics"""
from nowcasting_datamodel.models import DatetimeInterval, ForecastValueLatestSQL, GSPYieldSQL


def filter_query_on_datetime_interval(datetime_interval: DatetimeInterval, query):
    """
    Filter the query on the datetime interval

    :param datetime_interval: the datetime interval object
    :param query: sql query
    :return: query
    """
    query = query.filter(ForecastValueLatestSQL.target_time > datetime_interval.start_datetime_utc)
    query = query.filter(ForecastValueLatestSQL.target_time <= datetime_interval.end_datetime_utc)
    query = query.filter(GSPYieldSQL.datetime_utc == ForecastValueLatestSQL.target_time)
    return query
