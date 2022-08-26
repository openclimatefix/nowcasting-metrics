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
    add_forecast_horizon_to_metric,
)
from nowcasting_metrics.utils import save_metric_value_to_database, get_all_forecast_horizons

logger = logging.getLogger(__name__)


latest_mae = Metric(
    name="Daily Latest MAE",
    description="This metric calculates the MAE for the latest OCF forecast "
    "and compares with the PVLive values. The data is from one day",
)

mae_forecast_horizon = Metric(
    name="Daily MAE",
    description="This metric calculates the MAE for the OCF forecast "
    "and compares with the PVLive values. The data is from one day.",
)


def make_mae_query(session, model: ForecastValueSQL | ForecastValueLatestSQL):
    """
    Make query for MAE
    :param session: database sessions
    :param model: which model to use, either ForecastValueSQL or ForecastValueLatestSQL
    :return:
    """

    return session.query(
        func.avg(
            model.expected_power_generation_megawatts - GSPYieldSQL.solar_generation_kw / 1000
        ),
        func.count(model.expected_power_generation_megawatts),
    )


def make_mae_one_gsp(
    session: Session, datetime_interval: DatetimeInterval, gsp_id: int
) -> (int, int):
    """
    Calculate the MAE for one GSP, and save to database

    :param session: database session
    :param datetime_interval: datetime interbal
    :param gsp_id: the gsp id
    :return: 1. the MAE, 2. the number of data points
    """

    logger.debug(
        f"Calculating MAE for last forecast for {gsp_id} for start={datetime_interval.end_datetime_utc} "
        f"and end-{datetime_interval.end_datetime_utc}"
    )

    query = make_mae_query(session=session, model=ForecastValueLatestSQL)

    query = filter_query_latest_on_datetime(
        query=query, gsp_id=gsp_id, datetime_interval=datetime_interval
    )

    results = query.all()

    number_of_data_points = results[0][1]
    value = results[0][0]

    logger.debug(f"Found MAE of {value} from {number_of_data_points} data points.")

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=latest_mae,
        location=get_location(gsp_id=gsp_id, session=session),
    )

    return value, number_of_data_points


def make_mae_one_gsp_forecast_horizon(
    session: Session,
    datetime_interval: DatetimeInterval,
    gsp_id: int,
    forecast_horizon_minutes: int,
) -> (int, int):
    """
    Calculate the MAE for one GSP for a given forecast horizon, and save to database

    :param session: database session
    :param datetime_interval: datetime interval
    :param gsp_id: the gsp id
    :return: 1. the MAE, 2. the number of data points
    """

    logger.debug(
        f"Calculating MAE for forecast for {gsp_id}, {forecast_horizon_minutes=} "
        f"for start={datetime_interval.end_datetime_utc} "
        f"and end-{datetime_interval.end_datetime_utc}"
    )

    query = make_mae_query(session=session, model=ForecastValueSQL)

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

    metric = add_forecast_horizon_to_metric(
        metric=mae_forecast_horizon, forecast_horizon_minutes=forecast_horizon_minutes
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


def make_mae(session: Session, datetime_interval: DatetimeInterval, n_gsps: Optional[int] = N_GSP):
    """
    Calculate MAE for all GSPs

    :param session: database session
    :param datetime_interval: datetime interval
    :param n_gsps: The number of Gsps to loop over. Default is N_GSP. (+1 for national)
    """

    for gps_id in range(0, n_gsps + 1):
        make_mae_one_gsp(session=session, datetime_interval=datetime_interval, gsp_id=gps_id)

    for forecast_horizon_minutes in get_all_forecast_horizons():
        for gps_id in range(0, n_gsps + 1):
            make_mae_one_gsp_forecast_horizon(
                session=session,
                datetime_interval=datetime_interval,
                gsp_id=gps_id,
                forecast_horizon_minutes=forecast_horizon_minutes,
            )
