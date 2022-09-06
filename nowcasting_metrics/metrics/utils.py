""" util functions for metrics"""
from nowcasting_datamodel.models import (
    DatetimeInterval,
    ForecastValueLatestSQL,
    GSPYieldSQL,
    ForecastValueSQL,
    ForecastSQL,
    LocationSQL,
)
from sqlalchemy import text


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


def make_forecast_sub_query(datetime_interval, forecast_horizon_minutes, gsp_id, session):
    """
    Make SQL sub query to get latest forecast, given a forecast horizon

    This filters the ForecastValueSQL, and filters on
    - target_time
    - gsp_id
    - created_utc

    :param datetime_interval: the datetime interval
    :param forecast_horizon_minutes: the forecast horizon
        ie. Use results from forecast that are made 60 minutes before target time
    :param gsp_id: the gsp id
    :param session: database sessions
    :return: sub query
    """
    # make forecast sub query
    sub_query_forecast = session.query(ForecastValueSQL.id)
    sub_query_forecast = sub_query_forecast.distinct(ForecastValueSQL.target_time)
    sub_query_forecast = sub_query_forecast.join(ForecastSQL)
    sub_query_forecast = sub_query_forecast.join(ForecastSQL.location)
    sub_query_forecast = sub_query_forecast.filter(LocationSQL.gsp_id == gsp_id)
    # this seems to only work for postgres
    sub_query_forecast = sub_query_forecast.filter(
        ForecastValueSQL.target_time - ForecastValueSQL.created_utc
        >= text(f"interval '{forecast_horizon_minutes} minute'")
    )
    sub_query_forecast = sub_query_forecast.filter(
        ForecastValueSQL.target_time > datetime_interval.start_datetime_utc
    )
    sub_query_forecast = sub_query_forecast.filter(
        ForecastValueSQL.target_time <= datetime_interval.end_datetime_utc
    )
    sub_query_forecast = sub_query_forecast.order_by(
        ForecastValueSQL.target_time, ForecastValueSQL.created_utc.desc()
    )
    sub_query_forecast = sub_query_forecast.subquery()
    return sub_query_forecast


def make_gsp_sub_query(datetime_interval, gsp_id, session):
    """
    Get the GSP yeilds for a give datetime interval

    :param datetime_interval:
    :param gsp_id: the gsp id
    :param session: sql session
    :return: sub query
    """
    # Make gsp subquery
    sub_query_gsp = session.query(GSPYieldSQL.id)
    sub_query_gsp = sub_query_gsp.join(GSPYieldSQL.location)
    sub_query_gsp = sub_query_gsp.filter(LocationSQL.gsp_id == gsp_id)
    sub_query_gsp = sub_query_gsp.filter(
        GSPYieldSQL.datetime_utc > datetime_interval.start_datetime_utc
    )
    sub_query_gsp = sub_query_gsp.filter(
        GSPYieldSQL.datetime_utc <= datetime_interval.end_datetime_utc
    )
    sub_query_gsp = sub_query_gsp.filter(GSPYieldSQL.regime == "day-after")
    sub_query_gsp = sub_query_gsp.subquery()
    return sub_query_gsp
