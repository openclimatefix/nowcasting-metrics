""" General util functions """
import datetime
import logging
from typing import Optional

from nowcasting_datamodel.models.gsp import LocationSQL
from nowcasting_datamodel.models.metric import DatetimeInterval, DatetimeIntervalSQL, Metric, MetricSQL, MetricValueSQL
from nowcasting_datamodel.read.read_metric import get_datetime_interval, get_metric
from nowcasting_datamodel.read.read_models import get_model

logger = logging.getLogger(__name__)


def save_metric_value_to_database(
    session,
    value: float,
    number_of_data_points: int,
    metric: Metric | MetricSQL,
    datetime_interval: DatetimeInterval | DatetimeIntervalSQL,
    location: Optional[LocationSQL] = None,
    forecast_horizon_minutes: Optional[int] = None,
    time_of_day: Optional[datetime.time] = None,
    model_name: Optional[str] = None,
    plevel: Optional[float] = None,
):
    """
    Save one metric value to the database

    :param session: database sesstion
    :param value: metric value
    :param number_of_data_points: how many data points went into the metric
    :param metric: the metric object
    :param datetime_interval: datetime interval for the metric
    :param location: location object of the metric value
    :param forecast_horizon_minutes: the forecast horizon of the forecast. This is optional.
    :param time_of_day: the time of day of the forecast. This is optional.
    :param model_name: the model name of the forecast. This is optional.
    """

    if value is None:
        logger.warning(
            f"Cant not add metric as value is None "
            f"{metric.name=} "
            f"{datetime_interval.start_datetime_utc=} "
            f"{datetime_interval.end_datetime_utc=} "
            f"{forecast_horizon_minutes=}")
        if location is not None:
            logger.warning(f"{location.gsp_id=}")

    else:

        if type(metric) is Metric:
            metric_sql = get_metric(session=session, name=metric.name)
        else:
            metric_sql = metric

        if type(datetime_interval) is DatetimeInterval:
            datetime_interval_sql = get_datetime_interval(
                session=session,
                start_datetime_utc=datetime_interval.start_datetime_utc,
                end_datetime_utc=datetime_interval.end_datetime_utc,
            )
        else:
            datetime_interval_sql = datetime_interval

        metric_value_sql = MetricValueSQL(
            value=value,
            number_of_data_points=number_of_data_points,
            metric=metric_sql,
            datetime_interval=datetime_interval_sql,
        )

        if forecast_horizon_minutes is not None:
            metric_value_sql.forecast_horizon_minutes = forecast_horizon_minutes

        if location is not None:
            metric_value_sql.location = location

        if time_of_day is not None:
            metric_value_sql.time_of_day = time_of_day

        if model_name is not None:
            model = get_model(session=session, name=model_name)
            metric_value_sql.model_id = model.id

        if plevel is not None:
            metric_value_sql.p_level = plevel

        session.add(metric_value_sql)
