""" util functions for metrics"""
from nowcasting_datamodel.models import (
    DatetimeInterval,
    ForecastSQL,
    ForecastValueLatestSQL,
    ForecastValueSevenDaysSQL,
    ForecastValueFourteenDaysSQL,  # <-- Make sure this exists in nowcasting_datamodel
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
default_national_models = ["cnn", "pvnet_v2", "National_xg", "pvnet_day_ahead", "neso-solar-forecast"]
default_probabilistic_models = ["pvnet_v2", "National_xg", "pvnet_day_ahead"]


def get_forecast_range(max_forecast_horizon_minutes) -> list[int]:
    """
    Get the forecast range.

    0-4 hours is in 30-minute increments, then 1-hour increments thereafter.
    """
    if max_forecast_horizon_minutes <= 480:
        return list(range(0, max_forecast_horizon_minutes, 30))
    else:
        forecast_range_4 = list(range(0, 480, 30))
        forecast_range_ge_4 = list(range(480, max_forecast_horizon_minutes, 60))
        return forecast_range_4 + forecast_range_ge_4


def filter_query_on_datetime_interval(datetime_interval: DatetimeInterval, query):
    """
    Filter the query on the given DatetimeInterval.
    """
    query = query.filter(ForecastValueLatestSQL.target_time > datetime_interval.start_datetime_utc)
    query = query.filter(ForecastValueLatestSQL.target_time <= datetime_interval.end_datetime_utc)
    query = query.filter(GSPYieldSQL.datetime_utc == ForecastValueLatestSQL.target_time)
    return query


def make_forecast_sub_query(
    datetime_interval: DatetimeInterval,
    forecast_horizon_minutes: int,
    gsp_id: int,
    session: Session,
    model_name: str = "cnn",
):
    """
    Make an SQL subquery to get the 'latest forecast' for a given forecast horizon.

    This filters ForecastValueSQL on:
      - target_time
      - gsp_id
      - created_utc
    and orders by (target_time, created_utc DESC).

    :param datetime_interval: The datetime range to filter on
    :param forecast_horizon_minutes: How many minutes before the target_time the forecast was created
    :param gsp_id: The GSP ID to filter on
    :param session: The SQLAlchemy session
    :param model_name: The name of the model (e.g. "cnn", "neso-solar-forecast", etc.)
    :return: A subquery object
    """

    # Choose which ForecastValue model to use based on model_name
    if model_name == "neso-solar-forecast":
        model = ForecastValueFourteenDaysSQL
    else:
        model = ForecastValueSevenDaysSQL

    # Build the subquery
    sub_query_forecast = session.query(model.uuid)
    sub_query_forecast = sub_query_forecast.distinct(model.target_time)
    sub_query_forecast = sub_query_forecast.join(ForecastSQL)
    sub_query_forecast = sub_query_forecast.join(ForecastSQL.location)
    sub_query_forecast = sub_query_forecast.join(ForecastSQL.model)
    sub_query_forecast = sub_query_forecast.filter(LocationSQL.gsp_id == gsp_id)

    # Only use forecasts that were created at least X minutes before their target_time
    sub_query_forecast = sub_query_forecast.filter(
        model.target_time - model.created_utc >= text(f"interval '{forecast_horizon_minutes} minute'")
    )

    # Exclude forecasts created too close to the start date
    sub_query_forecast = sub_query_forecast.filter(
        datetime_interval.start_datetime_utc - model.created_utc
        < text(f"interval '{forecast_horizon_minutes} minute'")
    )

    # Limit how far back in time we look for forecasts
    sub_query_forecast = sub_query_forecast.filter(
        datetime_interval.start_datetime_utc - ForecastSQL.created_utc
        < text(f"interval '{forecast_horizon_minutes} minute' + interval '1 day'")
    )

    # Target times must be within the datetime interval
    sub_query_forecast = sub_query_forecast.filter(model.target_time > datetime_interval.start_datetime_utc)
    sub_query_forecast = sub_query_forecast.filter(model.target_time <= datetime_interval.end_datetime_utc)

    # Filter by the model name
    sub_query_forecast = sub_query_forecast.filter(MLModelSQL.name == model_name)

    # Sort by target_time ascending, created_utc descending
    sub_query_forecast = sub_query_forecast.order_by(model.target_time, model.created_utc.desc())

    # Return the subquery
    return sub_query_forecast.subquery()


def make_gsp_sub_query(datetime_interval: DatetimeInterval, gsp_id: int, session: Session):
    """
    Get the GSP yields for a given datetime interval.
    """
    sub_query_gsp = session.query(GSPYieldSQL.id)
    sub_query_gsp = sub_query_gsp.join(GSPYieldSQL.location)
    sub_query_gsp = sub_query_gsp.filter(LocationSQL.gsp_id == gsp_id)
    sub_query_gsp = sub_query_gsp.filter(GSPYieldSQL.datetime_utc > datetime_interval.start_datetime_utc)
    sub_query_gsp = sub_query_gsp.filter(GSPYieldSQL.datetime_utc <= datetime_interval.end_datetime_utc)
    sub_query_gsp = sub_query_gsp.filter(GSPYieldSQL.regime == "day-after")
    return sub_query_gsp.subquery()


def make_pvlive_subquery(
    session: Session, datetime_interval: DatetimeInterval, regime: str, gsp_id: int
):
    """
    Make a PV 'live' query for in-day data.
    """
    query = session.query(GSPYieldSQL)
    query = query.filter(GSPYieldSQL.datetime_utc >= datetime_interval.start_datetime_utc)
    query = query.filter(GSPYieldSQL.datetime_utc < datetime_interval.end_datetime_utc)
    query = query.filter(GSPYieldSQL.regime == regime)

    # Filter on location
    query = query.join(LocationSQL)
    query = query.filter(LocationSQL.gsp_id == gsp_id)

    return query
