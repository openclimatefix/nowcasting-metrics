""" Look at probabilistic metrics for nowcasting models

 We will look at
 - exceedence, the amount of times the values it over the plevel. We would expect 90% of valyes to be over the 10% plevel
 - pinball loss

 We will look at p levels 10 and 90
 for different forecast horizons

 """

import logging
from typing import Optional, Dict
from nowcasting_datamodel.models import (
    ForecastValueSevenDaysSQL,
    DatetimeInterval,
    GSPYieldSQL,
    Metric,
)
from nowcasting_datamodel.read.read import get_location
from sqlalchemy import text
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import func
from sqlalchemy import Float

from nowcasting_metrics.metrics.utils import make_gsp_sub_query, make_forecast_sub_query
from nowcasting_metrics.utils import save_metric_value_to_database

logger = logging.getLogger(__name__)

pinball = Metric(
    name="Pinball loss",
    description="Pin ball loss for probabilistic forecast. This is for one p level value",
)

exceedance = Metric(
    name="Exceedance",
    description="The percentage of times the forecast is over the p level. This is for one p level value",
)


def make_probabilistic_metrics_one_forecast_horizon_minutes(
    session,
    model_name: str,
    datetime_interval: DatetimeInterval,
    forecast_horizon_minutes: int,
    p_level: str,
):
    """Make probabilistic metrics for one forecast horizon minutes

    Args:
    session: sqlalchemy session
    model_name: name of the model
    datetime_interval: datetime interval to look at
    forecast_horizon_minutes: forecast horizon minutes to look at
    p_level: p levels to look at
    """

    logger.info(
        f"Making pinball and exceedance metrics for probabilistic forecast, "
        f"for {model_name}, {forecast_horizon_minutes}, {p_level}"
    )

    # get a valye between 0 and 1
    tau = float(p_level) / 100

    # get truth and prediction sub queries
    sub_query_gsp = make_gsp_sub_query(
        datetime_interval=datetime_interval, gsp_id=0, session=session
    )
    sub_query_forecast = make_forecast_sub_query(
        datetime_interval=datetime_interval,
        forecast_horizon_minutes=forecast_horizon_minutes,
        gsp_id=0,
        session=session,
        model=ForecastValueSevenDaysSQL,
        model_name=model_name,
    )

    # join truth and predictions together
    query = session.query(
        func.count(),
    )
    query = query.filter(ForecastValueSevenDaysSQL.uuid.in_(sub_query_forecast))
    query = query.filter(GSPYieldSQL.id.in_(sub_query_gsp))
    query = query.filter(GSPYieldSQL.datetime_utc == ForecastValueSevenDaysSQL.target_time)

    results_count = query.all()

    # get the number of times the prediction is over the p level
    query = session.query(
        func.avg(
            ForecastValueSevenDaysSQL.properties[p_level].as_float()
            - GSPYieldSQL.solar_generation_kw / 1000
        ),
        func.count(),
    )
    query = query.filter(ForecastValueSevenDaysSQL.uuid.in_(sub_query_forecast))
    query = query.filter(GSPYieldSQL.id.in_(sub_query_gsp))
    query = query.filter(GSPYieldSQL.datetime_utc == ForecastValueSevenDaysSQL.target_time)
    query = query.filter(
        ForecastValueSevenDaysSQL.properties[p_level].as_float()
        > GSPYieldSQL.solar_generation_kw / 1000
    )
    results_over = query.all()

    # get the number of times the prediction is under the p level
    query = session.query(
        func.avg(
            GSPYieldSQL.solar_generation_kw / 1000
            - ForecastValueSevenDaysSQL.properties[p_level].as_float()
        ),
        func.count(),
    )
    query = query.filter(ForecastValueSevenDaysSQL.uuid.in_(sub_query_forecast))
    query = query.filter(GSPYieldSQL.id.in_(sub_query_gsp))
    query = query.filter(GSPYieldSQL.datetime_utc == ForecastValueSevenDaysSQL.target_time)
    query = query.filter(
        ForecastValueSevenDaysSQL.properties[p_level].as_float()
        <= GSPYieldSQL.solar_generation_kw / 1000
    )
    results_under = query.all()

    number_of_data_points = results_count[0][0]

    # if there are no values under, then `results_under' is None, hence we need to check
    # similar fo values over
    pinball_value = 0
    if results_under[0][0] is not None:
        pinball_value += (results_under[0][0] * results_under[0][1]) * (1 - tau)
    if results_over[0][0] is not None:
        pinball_value += (results_over[0][0] * results_over[0][1]) * tau

    # divide by the number of data points to get average
    pinball_value = pinball_value / number_of_data_points
    exceedance_value = results_over[0][1] / number_of_data_points

    print(f"results_under: {results_under}")
    print(f"results_over: {results_over}")
    print(f"number_of_data_points: {number_of_data_points}")
    print(f"pinball: {pinball_value}")
    print(f"tau: {tau}")

    # save to database
    save_metric_value_to_database(
        session=session,
        value=pinball_value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=pinball,
        location=get_location(gsp_id=0, session=session),
        model_name=model_name,
        plevel=float(p_level),
    )

    # save to database
    save_metric_value_to_database(
        session=session,
        value=exceedance_value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=exceedance,
        location=get_location(gsp_id=0, session=session),
        model_name=model_name,
        plevel=float(p_level),
    )

    return exceedance_value, pinball_value, number_of_data_points


def make_probabilistic(
    session: Session,
    datetime_interval: DatetimeInterval,
    max_forecast_horizon_minutes: Optional[Dict[str, int]] = None,
):
    """
    Make make_probabilistic for all models and forecast horizons

    :param session: database session
    :param datetime_interval: datetime interval
    :param max_forecast_horizon_minutes: max forecast horizon minutes for each model.
    :return: None
    """

    if max_forecast_horizon_minutes is None:
        max_forecast_horizon_minutes = {"National_xg": 40 * 60, "pvnet_v2": 480}

    for model_name in ["pvnet_v2", "National_xg"]:
        for forecast_horizon_minute in max_forecast_horizon_minutes[model_name]:
            for p_level in ["10", "90"]:

                make_probabilistic_metrics_one_forecast_horizon_minutes(
                    session=session,
                    model_name=model_name,
                    datetime_interval=datetime_interval,
                    forecast_horizon_minutes=forecast_horizon_minute,
                    p_level=p_level,
                )
