from nowcasting_datamodel.models import Metric
from nowcasting_datamodel.models.gsp import GSPYieldSQL, LocationSQL
from nowcasting_datamodel.models.metric import DatetimeInterval
from nowcasting_datamodel.models.models import ForecastValueLatestSQL, ForecastValueSQL, ForecastSQL
from sqlalchemy.sql import and_, text


def filter_query_latest_on_datetime(query, gsp_id: int, datetime_interval: DatetimeInterval):
    """

    Filter a sql query on datetime, and gsp id and regime

    :param query: sql query
    :param gsp_id: the gsp id
    :param datetime_interval: the datetime interval to filter on
    :return: query with these filter
    """

    # filter on gsp
    query = query.filter()

    # filter on location
    query = query.join(GSPYieldSQL.location)
    query = query.filter(LocationSQL.gsp_id == gsp_id)
    query = query.filter(ForecastValueLatestSQL.gsp_id == gsp_id)

    # join target time and yield
    query = query.filter(ForecastValueLatestSQL.target_time > datetime_interval.start_datetime_utc)
    query = query.filter(ForecastValueLatestSQL.target_time <= datetime_interval.end_datetime_utc)
    query = query.filter(GSPYieldSQL.datetime_utc == ForecastValueLatestSQL.target_time)

    # filter on gsp regime
    query = query.filter(GSPYieldSQL.regime == "day-after")

    return query


def filter_query_on_datetime_forecast_horizon(
    query, gsp_id: int, datetime_interval: DatetimeInterval, forecast_horizon_minutes: int
):
    """

    Filter a sql query on datetime, and gsp id and regime

    :param query: sql query
    :param gsp_id: the gsp id
    :param datetime_interval: the datetime interval to filter on
    :param forecast_horizon_minutes: the forecast horizon in minutes
    :return: query with these filter
    """

    # TODO need to do a sub query and then take do metric

    # filter on gsp
    query = query.distinct(ForecastValueSQL.target_time)

    # join target time and yield
    query = query.filter(ForecastValueSQL.target_time > datetime_interval.start_datetime_utc)
    query = query.filter(ForecastValueSQL.target_time <= datetime_interval.end_datetime_utc)
    query = query.filter(GSPYieldSQL.datetime_utc == ForecastValueSQL.target_time)

    # filter on location
    query = query.join(ForecastSQL.forecast_values)
    query = query.join(
        LocationSQL,
        and_(GSPYieldSQL.location_id == LocationSQL.id, ForecastSQL.location_id == LocationSQL.id),
    )
    query = query.filter(LocationSQL.gsp_id == gsp_id)

    # filter on creation time
    # this seems to only work for postgres
    query = query.filter(
        ForecastValueSQL.target_time - ForecastValueSQL.created_utc
        >= text(f"interval '{forecast_horizon_minutes} minute'")
    )

    # filter on gsp regime
    query = query.filter(GSPYieldSQL.regime == "day-after")

    query = query.order_by(ForecastValueSQL.target_time, ForecastValueSQL.created_utc.desc())

    return query


def add_forecast_horizon_to_metric(metric: Metric, forecast_horizon_minutes: int):
    """
    Add forecast horizon to metric name and description

    :param metric: the orginal metric
    :param forecast_horizon_minutes: what the forecast horizon in minutes is
    """

    metric_copy = Metric(**metric.dict())
    metric_copy.name = metric_copy.name + f" Forecast Horizon {forecast_horizon_minutes} minutes"
    metric_copy.description = (
        metric_copy.description
        + f"This if for a forecast horizon of {forecast_horizon_minutes} minutes"
    )

    return metric_copy
