""" Look at probabilistic metrics for nowcasting models

 We will look at
 - exceedence, the amount of times the values it over the plevel. We would expect 90% of valyes to be over the 10% plevel
 - pinball loss

 We will look at p levels 10 and 90
 for different forecast horizons

 """

import logging
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

    from sqlalchemy.dialects import postgresql

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
            GSPYieldSQL.solar_generation_kw / 1000 - ForecastValueSevenDaysSQL.properties[p_level].as_float()
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
    exceedance = results_over[0][1] / number_of_data_points

    pinball = 0
    if results_under[0][0] is not None:
        pinball += (results_under[0][0] * results_under[0][1]) * (1 - tau)
    if results_over[0][0] is not None:
        pinball += (results_over[0][0] * results_over[0][1]) * tau

    pinball = pinball/number_of_data_points

    print(f"results_under: {results_under}")
    print(f"results_over: {results_over}")
    print(f"number_of_data_points: {number_of_data_points}")
    print(f"pinball: {pinball}")
    print(f"tau: {tau}")

    # save to database


    return exceedance, pinball, number_of_data_points
