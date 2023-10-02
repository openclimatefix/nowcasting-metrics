""" Function to make MAE """
import logging
from typing import Optional, Union

from nowcasting_datamodel.models import ForecastValueLatestSQL, ForecastValueSevenDaysSQL, Metric
from nowcasting_datamodel.models.gsp import GSPYieldSQL
from nowcasting_datamodel.models.metric import DatetimeInterval
from nowcasting_datamodel.read.read import get_location
from sqlalchemy import Time, cast
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import func

from nowcasting_metrics.metrics.utils import make_forecast_sub_query, make_gsp_sub_query
from nowcasting_metrics.utils import save_metric_value_to_database

logger = logging.getLogger(__name__)


me_hh = Metric(
    name="Half Hourly ME",
    description="The mean error for a given half hour interval over the a number of days",
)


def make_me_one_gsp_with_forecast_horizon_and_one_half_hour(
    session: Session,
    datetime_interval: DatetimeInterval,
    gsp_id: int,
    forecast_horizon_minutes: int,
    model_name: str = None,
    save_to_database: bool = True,
) -> (int, int):
    """
    Calculate the ME for one GSP for a forecast horizon for one half hour, and save to database

    :param session: database session
    :param datetime_interval: datetime interval
    :param gsp_id: the gsp id
    :param forecast_horizon_minutes: the forecast horizon ie. Use results from forecast that are
        made 60 minutes before target time
    :param model_name: the model name of the forecast. This is optional.
    :param save_to_database: if True, save the results to the database
    :return: 1. the MAE, 2. the number of data points
    """

    sub_query_gsp = make_gsp_sub_query(datetime_interval, gsp_id, session)
    sub_query_forecast = make_forecast_sub_query(
        datetime_interval=datetime_interval,
        forecast_horizon_minutes=forecast_horizon_minutes,
        gsp_id=gsp_id,
        session=session,
        model_name=model_name,
    )

    # make full query
    query = make_me_query(session, model=ForecastValueSevenDaysSQL)

    query = query.filter(ForecastValueSevenDaysSQL.uuid.in_(sub_query_forecast))
    query = query.filter(GSPYieldSQL.id.in_(sub_query_gsp))
    query = query.filter(GSPYieldSQL.datetime_utc == ForecastValueSevenDaysSQL.target_time)

    # group by by time of day
    query = query.group_by(cast(GSPYieldSQL.datetime_utc, Time))
    results = query.all()

    for result in results:
        number_of_data_points = result[1]
        value = result[0]
        time_of_day = result[2]

        logger.debug(
            f"Found ME of {value} from {number_of_data_points} "
            f"data points for forecast horizon {forecast_horizon_minutes} for "
            f"{gsp_id=} and {time_of_day=}. This is for {model_name=}."
        )
        if save_to_database:
            save_metric_value_to_database(
                session=session,
                value=value,
                number_of_data_points=number_of_data_points,
                datetime_interval=datetime_interval,
                time_of_day=time_of_day,
                metric=me_hh,
                location=get_location(gsp_id=gsp_id, session=session),
                forecast_horizon_minutes=forecast_horizon_minutes,
                model_name=model_name
            )

    return results


def make_me_query(
    session,
    model: Union[ForecastValueSevenDaysSQL, ForecastValueLatestSQL] = ForecastValueLatestSQL,
):
    """
    Make ME query

    :param session: database sessions
    :param model: either ForecastValueSQL or ForecastValueLatestSQL
    :return: query
    """
    query = session.query(
        func.avg(
            model.expected_power_generation_megawatts - GSPYieldSQL.solar_generation_kw / 1000
        ),
        func.count(model.expected_power_generation_megawatts),
        cast(GSPYieldSQL.datetime_utc, Time),
    )
    return query


def make_me(
    session: Session,
    datetime_interval: DatetimeInterval,
    max_forecast_horizon_minutes: Optional[dict] = None,
):
    """
    Calculate MAE for all GSPs

    :param max_forecast_horizon_minutes:
    :param session: database session
    :param datetime_interval: datetime interval
    :param: max_forecast_horizon_minutes.
        The maximum forecast horizon we should look at, default is 8 hours
    """

    if max_forecast_horizon_minutes is None:
        max_forecast_horizon_minutes = {"cnn": 480, "National_xg": 40*60, "pvnet_v2": 480}

    # loop over forecast horizons
    for model_name in ["cnn", "pvnet_v2", "National_xg"]:
        for forecast_horizon_minutes in range(0, max_forecast_horizon_minutes[model_name], 30):
            make_me_one_gsp_with_forecast_horizon_and_one_half_hour(
                session=session,
                datetime_interval=datetime_interval,
                gsp_id=0,
                forecast_horizon_minutes=forecast_horizon_minutes,
                model_name=model_name,
            )
