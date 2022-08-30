""" Function to make RMSE """
import logging
from typing import Optional

from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.models import Metric
from nowcasting_datamodel.models.gsp import GSPYieldSQL, LocationSQL
from nowcasting_datamodel.models.metric import DatetimeInterval
from nowcasting_datamodel.models.models import ForecastValueLatestSQL
from nowcasting_datamodel.read.read import get_location
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import func

from nowcasting_metrics.metrics.utils import filter_query_on_datetime_interval
from nowcasting_metrics.utils import save_metric_value_to_database

logger = logging.getLogger(__name__)


latest_rmse = Metric(
    name="Daily Latest RMSE",
    description="This metric calculates the RMSE for the latest OCF forecast "
    "and compares with the PVLive values. The data is from one day",
)

rmse_all_gsps = Metric(
    name="Daily Latest RMSE All GSPs",
    description="This metric calculates the RMSE for the latest OCF forecast "
    "and compares with the PVLive values. The data is from one day. "
    "This is for all GSPs (not the national)",
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
        f"Calculating RMSE for last forecast for {gsp_id=} "
        f"for start={datetime_interval.end_datetime_utc} "
        f"and end-{datetime_interval.end_datetime_utc}"
    )

    query = make_rmse_query(session)

    # filter on gsp
    query = query.filter()

    # filter on target time
    query = query.join(GSPYieldSQL.location)
    query = query.filter(LocationSQL.gsp_id == gsp_id)
    query = query.filter(ForecastValueLatestSQL.gsp_id == gsp_id)

    # join target time and yield
    query = filter_query_on_datetime_interval(datetime_interval, query)

    # filter on gsp regime
    query = query.filter(GSPYieldSQL.regime == "day-after")

    results = query.all()

    number_of_data_points = results[0][1]
    value = results[0][0] ** 0.5

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


def make_rmse_all_gsp(session: Session, datetime_interval: DatetimeInterval):
    """
    Calculate the RMSE for all GSP (not national), and save to database

    :param session: database session
    :param datetime_interval: datetime interbal
    :return: 1. the MAE, 2. the number of data points
    """

    logger.debug(
        f"Calculating RMSE for last forecast for all gsps (not national)"
        f"for start={datetime_interval.end_datetime_utc} "
        f"and end-{datetime_interval.end_datetime_utc}"
    )

    query = make_rmse_query(session)

    # filter on gsp
    query = query.filter()

    # filter on target time
    query = query.join(GSPYieldSQL.location)
    query = query.filter(LocationSQL.gsp_id != 0)
    query = query.filter(ForecastValueLatestSQL.gsp_id != 0)
    query = query.filter(ForecastValueLatestSQL.gsp_id == LocationSQL.gsp_id)

    # join target time and yield
    query = filter_query_on_datetime_interval(datetime_interval, query)

    # filter on gsp regime
    query = query.filter(GSPYieldSQL.regime == "day-after")

    results = query.all()

    number_of_data_points = results[0][1]
    value = results[0][0] ** 0.5

    logger.debug(f"Found RMSE of {value} from {number_of_data_points} data points.")

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=rmse_all_gsps,
    )

    return value, number_of_data_points


def make_rmse_query(session):
    """
    Make rmse query

    :param session: database sessions
    :return: query
    """
    query = session.query(
        func.avg(
            func.pow(
                ForecastValueLatestSQL.expected_power_generation_megawatts
                - GSPYieldSQL.solar_generation_kw / 1000,
                2,
            )
        ),
        func.count(ForecastValueLatestSQL.expected_power_generation_megawatts),
    )
    return query


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

    make_rmse_all_gsp(session=session, datetime_interval=datetime_interval)
