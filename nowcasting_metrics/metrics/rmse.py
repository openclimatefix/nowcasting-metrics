import logging
from typing import Optional

from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.models import Metric
from nowcasting_datamodel.models.gsp import GSPYieldSQL
from nowcasting_datamodel.models.metric import DatetimeInterval
from nowcasting_datamodel.models.models import ForecastValueLatestSQL, ForecastValueSQL
from nowcasting_datamodel.read.read import get_location
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import func

from nowcasting_metrics.metrics.utils import (
    filter_query_latest_on_datetime,
    filter_query_on_datetime_forecast_horizon,
)
from nowcasting_metrics.utils import save_metric_value_to_database

logger = logging.getLogger(__name__)


latest_rmse = Metric(
    name="Daily Latest RMSE",
    description="This metric calculates the RMSE for the latest OCF forecast "
    "and compares with the PVLive values. The data is from one day",
)

rmse_forecast_horizon = Metric(
    name="Daily RMSE",
    description="This metric calculates the RMSE for the OCF forecast "
    "and compares with the PVLive values. The data is from one day.",
)


def make_rmse_query(session, model: ForecastValueSQL | ForecastValueLatestSQL):
    """
    Make query for RMSE
    :param session: database sessions
    :param model: which model to use, either ForecastValueSQL or ForecastValueLatestSQL
    :return:
    """

    return session.query(
        func.avg(
            func.pow(
                model.expected_power_generation_megawatts - GSPYieldSQL.solar_generation_kw / 1000,
                2,
            )
        ),
        func.count(model.expected_power_generation_megawatts),
    )


def make_rmse_one_gsp(session: Session, datetime_interval: DatetimeInterval, gsp_id: int):
    """
    Calculate the RMSE for one GSP, and save to database

    :param session: database session
    :param datetime_interval: datetime interbal
    :param gsp_id: the gsp id
    :return: 1. the MAE, 2. the number of data points
    """

    logger.debug(
        f"Calculating RMSE for last forecast for {gsp_id} "
        f"for start={datetime_interval.end_datetime_utc} "
        f"and end-{datetime_interval.end_datetime_utc}"
    )

    query = make_rmse_query(session=session, model=ForecastValueLatestSQL)

    query = filter_query_latest_on_datetime(
        query=query, gsp_id=gsp_id, datetime_interval=datetime_interval
    )

    results = query.all()

    logger.debug(results)

    number_of_data_points = results[0][1]
    value = results[0][0] * 0.5

    logger.debug(f"Found RMSE of {value} from {number_of_data_points} data points.")

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=latest_rmse,
        location=get_location(gsp_id=gsp_id, session=session),
    )

    return value, number_of_data_points


def make_rmse_one_gsp_forecast_horizon(
    session: Session,
    datetime_interval: DatetimeInterval,
    gsp_id: int,
    forecast_horizon_minutes: int,
) -> (int, int):
    """
    Calculate the RMSE for one GSP for a given forecast horizon, and save to database

    :param session: database session
    :param datetime_interval: datetime interval
    :param gsp_id: the gsp id
    :return: 1. the MAE, 2. the number of data points
    """

    logger.debug(
        f"Calculating RMSE for forecast for {gsp_id}, {forecast_horizon_minutes=} "
        f"for start={datetime_interval.end_datetime_utc} "
        f"and end-{datetime_interval.end_datetime_utc}"
    )

    query = make_rmse_query(session=session, model=ForecastValueSQL)

    # filter query
    query = filter_query_on_datetime_forecast_horizon(
        query=query,
        gsp_id=gsp_id,
        datetime_interval=datetime_interval,
        forecast_horizon_minutes=forecast_horizon_minutes,
    )

    results = query.all()
    logger.debug(results)

    number_of_data_points = results[0][1]
    value = results[0][0]

    logger.debug(f"Found MAE of {value} from {number_of_data_points} data points.")

    metric = rmse_forecast_horizon
    metric.name = metric.name + f" Forecast Horizon {forecast_horizon_minutes} minutes"
    metric.description = (
        metric.description + f"This if for a forecast horizon of {forecast_horizon_minutes} minutes"
    )

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=metric,
        location=get_location(gsp_id=gsp_id, session=session),
    )

    return value, number_of_data_points


def make_rmse(session: Session, datetime_interval: DatetimeInterval, n_gsps: Optional[int] = N_GSP):
    """
    Calculate RMSE for all GSPs

    :param session: database session
    :param datetime_interval: datetime interval
    :param n_gsps: The number of gsps (+1 for national)
    """

    # loop over gsps
    for gps_id in range(0, n_gsps + 1):
        make_rmse_one_gsp(session=session, datetime_interval=datetime_interval, gsp_id=gps_id)
