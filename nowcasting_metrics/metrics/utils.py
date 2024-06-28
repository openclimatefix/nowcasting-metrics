""" util functions for metrics"""
from nowcasting_datamodel.models import (
    DatetimeInterval,
    ForecastSQL,
    ForecastValueLatestSQL,
    ForecastValueSevenDaysSQL,
    GSPYieldSQL,
    LocationSQL,
    MLModelSQL,
)
from sqlalchemy import text
from sqlalchemy.orm.session import Session


default_max_forecast_horizon_minutes = {
    "cnn": 480,
    "National_xg": 40 * 60,
    "pvnet_v2": 480,
    "pvnet_day_ahead": 40 * 60,
    "pvnet_gsp_sum": 480,
}

default_gsp_models = ["cnn", "pvnet_v2", "pvnet_day_ahead"]
default_national_models = ["cnn", "pvnet_v2", "National_xg", "pvnet_day_ahead"]
default_probabilistic_models = ["pvnet_v2", "National_xg", "pvnet_day_ahead"]


def get_forecast_range(max_forecast_horizon_minutes) -> list[int]:
    """
    Get the forecast range

    0-4 hours is in 30 minutes, and then in 1 hour blocks from there on

    :param max_forecast_horizon_minutes: the maximum forecast horizon
    :return: the forecast range
    """

    if max_forecast_horizon_minutes <= 480:
        return list(range(0, max_forecast_horizon_minutes, 30))
    else:
        forecast_range_4 = list(range(0, 480, 30))
        forecast_range_ge_4 = list(range(480, max_forecast_horizon_minutes, 60))
        return forecast_range_4 + forecast_range_ge_4


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


def make_forecast_sub_query(datetime_interval, forecast_horizon_minutes, gsp_id, session, model=ForecastValueSevenDaysSQL, model_name='cnn'):
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
    sub_query_forecast = session.query(model.uuid)
    sub_query_forecast = sub_query_forecast.distinct(model.target_time)
    sub_query_forecast = sub_query_forecast.join(ForecastSQL)
    sub_query_forecast = sub_query_forecast.join(ForecastSQL.location)
    sub_query_forecast = sub_query_forecast.join(ForecastSQL.model)
    sub_query_forecast = sub_query_forecast.filter(LocationSQL.gsp_id == gsp_id)

    # this seems to only work for postgres
    sub_query_forecast = sub_query_forecast.filter(
        model.target_time - model.created_utc
        >= text(f"interval '{forecast_horizon_minutes} minute'")
    )

    # if the start date is 2023-02-01 and horizon is 60 minutes,
    # then we want any older forecast than 2023-01-31 23:00:00
    sub_query_forecast = sub_query_forecast.filter(
        datetime_interval.start_datetime_utc - model.created_utc < text(f"interval '{forecast_horizon_minutes} minute'")
    )

    # only load relative new forecasts, stops looking over all forecasts
    # if the start date is 2023-02-01 and horizon is 60 minutes,
    # then we want any forecast that is newer than 2023-02-01 00:00:00 - 60 minutes - 1 day (buffer)
    sub_query_forecast = sub_query_forecast.filter(
        ForecastSQL.created_utc >
        datetime_interval.start_datetime_utc
        - text(f"interval '{forecast_horizon_minutes} minute'")
        - text(f"interval '1 day'")
    )
    sub_query_forecast = sub_query_forecast.filter(
        model.target_time > datetime_interval.start_datetime_utc
    )
    sub_query_forecast = sub_query_forecast.filter(
        model.target_time <= datetime_interval.end_datetime_utc
    )
    if model_name is not None:
        sub_query_forecast = sub_query_forecast.filter(MLModelSQL.name == model_name)
    sub_query_forecast = sub_query_forecast.order_by(
        model.target_time, model.created_utc.desc()
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


def make_pvlive_subquery(
    session: Session, datetime_interval: DatetimeInterval, regime: str, gsp_id: int
):
    """
    Make PV live query

    :param session: database sessions
    :param datetime_interval: which date interval to filer on
    :param regime: which regime to filter on
    :param gsp_id: which gsp_id to filer on
    """

    query = session.query(GSPYieldSQL)

    # sub query for in-day
    query = query.filter(GSPYieldSQL.datetime_utc >= datetime_interval.start_datetime_utc)
    query = query.filter(GSPYieldSQL.datetime_utc < datetime_interval.end_datetime_utc)
    query = query.filter(GSPYieldSQL.regime == regime)

    # filter on location
    query = query.join(LocationSQL)
    query = query.filter(LocationSQL.gsp_id == gsp_id)

    return query
