""" Function to make MAE """
import datetime
import logging
from typing import Optional, Union

from nowcasting_datamodel.models import ForecastValueLatestSQL, ForecastValueSQL, Metric
from nowcasting_datamodel.models.gsp import GSPYieldSQL
from nowcasting_datamodel.models.metric import DatetimeInterval
from nowcasting_datamodel.read.read import get_location
from sqlalchemy import Time, cast
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import func

from nowcasting_metrics.metrics.utils import (
    make_forecast_sub_query,
    make_gsp_sub_query,
)
from nowcasting_metrics.utils import save_metric_value_to_database

logger = logging.getLogger(__name__)


me_hh = Metric(
    name="Half Hourly ME",
    description="The mean error for a given half hour interval, " "over the a number of days",
)


def make_me_one_gsp_with_forecast_horizon_and_one_half_hour(
    session: Session,
    datetime_interval: DatetimeInterval,
    gsp_id: int,
    forecast_horizon_minutes: int,
    time_of_day: datetime.time,
) -> (int, int):
    """
    Calculate the ME for one GSP for a forecast horizon for one half hour, and save to database

    :param session: database session
    :param datetime_interval: datetime interval
    :param gsp_id: the gsp id
    :param forecast_horizon_minutes: the forecast horizon ie. Use results from forecast that are
        made 60 minutes before target time
    :param time_of_day: the time of the day
    :return: 1. the MAE, 2. the number of data points
    """

    sub_query_gsp = make_gsp_sub_query(datetime_interval, gsp_id, session)
    sub_query_forecast = make_forecast_sub_query(
        datetime_interval, forecast_horizon_minutes, gsp_id, session
    )

    # make full query
    query = make_me_query(session, model=ForecastValueSQL)

    query = query.filter(ForecastValueSQL.uuid.in_(sub_query_forecast))
    query = query.filter(GSPYieldSQL.id.in_(sub_query_gsp))
    query = query.filter(GSPYieldSQL.datetime_utc == ForecastValueSQL.target_time)

    # filter by time of day
    query = query.filter(cast(GSPYieldSQL.datetime_utc, Time) == time_of_day)
    results = query.all()

    number_of_data_points = results[0][1]
    value = results[0][0]

    logger.debug(
        f"Found MAE of {value} from {number_of_data_points} "
        f"data points for forecast horizon {forecast_horizon_minutes} for {gsp_id=}."
    )

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        time_of_dya=time_of_day,
        metric=me_hh,
        location=get_location(gsp_id=gsp_id, session=session),
        forecast_horizon_minutes=forecast_horizon_minutes,
    )

    return value, number_of_data_points


def make_me_query(
    session, model: Union[ForecastValueSQL, ForecastValueLatestSQL] = ForecastValueLatestSQL
):
    """
    Make ME query

    :param session: database sessions
    :param model: either ForecastValueSQL or ForecastValueLatestSQL
    :return: query
    """
    query = session.query(
        func.avg(model.expected_power_generation_megawatts - GSPYieldSQL.solar_generation_kw / 100),
        func.count(model.expected_power_generation_megawatts),
    )
    return query


def make_me(
    session: Session,
    datetime_interval: DatetimeInterval,
    max_forecast_horizon_minutes: Optional[int] = 480,
):
    """
    Calculate MAE for all GSPs

    :param max_forecast_horizon_minutes:
    :param session: database session
    :param datetime_interval: datetime interval
    :param: max_forecast_horizon_minutes.
        The maximum forecast horizon we should look at, default is 8 hours
    """

    # loop over forecast horizons and each half hour
    for forecast_horizon_minutes in range(0, max_forecast_horizon_minutes, 30):
        for hour in range(0, 24):
            for minute in [0, 30]:
                make_me_one_gsp_with_forecast_horizon_and_one_half_hour(
                    session=session,
                    datetime_interval=datetime_interval,
                    gsp_id=0,
                    time_of_day=datetime.time(hour, minute, 0),
                    forecast_horizon_minutes=forecast_horizon_minutes,
                )
