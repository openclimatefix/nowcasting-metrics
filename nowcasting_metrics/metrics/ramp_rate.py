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

from nowcasting_metrics.metrics.utils import default_national_models, make_gsp_sub_query, make_forecast_sub_query
from nowcasting_metrics.utils import save_metric_value_to_database

logger = logging.getLogger(__name__)

ramp_rate = Metric(
    name="Ramp rate MAE",
    description="This metric calculates the MAE ramp rate for the latest OCF forecast "
    "and compares with the PVLive values. Ramp rate is defined as from one forecast run "
    "((pred_{t+1 hour} - pred_{t}) - (true_{t+1 hour} - true_{t})) ."
    "We take the absolute value of the ramp rate and calculate the mean ",
)


def make_forecast_value_query(
    session,
    sub_query_name: str,
    model_name: str,
    datetime_interval: DatetimeInterval,
    forecast_horizon_minutes: int,
):
    """Make a query to get the forecast values and the true values from the database"""
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
        ForecastValueSevenDaysSQL.forecast_id.label(f"id_{sub_query_name}"),
        ForecastValueSevenDaysSQL.target_time.label(f"t_{sub_query_name}"),
        ForecastValueSevenDaysSQL.expected_power_generation_megawatts.label(
            f"power_{sub_query_name}"
        ),
        GSPYieldSQL.solar_generation_kw.label(f"true_{sub_query_name}"),
    )
    query = query.filter(ForecastValueSevenDaysSQL.uuid.in_(sub_query_forecast))
    query = query.filter(GSPYieldSQL.id.in_(sub_query_gsp))
    query = query.filter(GSPYieldSQL.datetime_utc == ForecastValueSevenDaysSQL.target_time)

    return query


def make_ramp_rate_one_forecast_horizon_minutes(
    session,
    model_name: str,
    datetime_interval: DatetimeInterval,
    forecast_horizon_minutes: int,
    ramp_rate_minutes: int,
):
    """Calculate one ramp rate metric for a given forecast horizon"""

    # get the forecast values with the forecast horizon
    query_a = make_forecast_value_query(
        session,
        "a",
        model_name,
        datetime_interval=datetime_interval,
        forecast_horizon_minutes=forecast_horizon_minutes,
    ).subquery()

    # get the forecast values with the forecast horizon + 60 minutes
    query_b = make_forecast_value_query(
        session,
        "b",
        model_name,
        datetime_interval=datetime_interval,
        forecast_horizon_minutes=forecast_horizon_minutes + ramp_rate_minutes,
    ).subquery()

    # # join queries together and get ramp rate
    query = session.query(
        func.avg(
            func.abs(query_b.c.power_b - query_a.c.power_a
            - (query_b.c.true_b / 1000 - query_a.c.true_a / 1000))
        ),
        func.count(),
    )

    # join queries together
    query = query.filter(query_a.c.id_a == query_b.c.id_b)
    query = query.filter(
        query_a.c.t_a == query_b.c.t_b - text(f"interval '{ramp_rate_minutes} minute'")
    )

    # get results
    results = query.all()
    number_of_data_points = results[0][1]
    value = results[0][0]

    logger.debug(
        f"Found Ramp Rate of {value} from {number_of_data_points} data points"
        f" for {forecast_horizon_minutes=} for gsp_id=0. {model_name=}"
    )

    # save to database
    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=ramp_rate,
        location=get_location(gsp_id=0, session=session),
        model_name=model_name,
        forecast_horizon_minutes=forecast_horizon_minutes,
    )

    return value, number_of_data_points


def make_ramp_rate(
    session: Session,
    datetime_interval: DatetimeInterval,
):
    """
    Make ramp rate for all models and forecast horizons

    :param session: database session
    :param datetime_interval: datetime interval
    :return: None
    """
    forecast_horizon_hours = [0, 1, 2]
    for forecast_horizon_hour in forecast_horizon_hours:
        for model_name in default_national_models:
            make_ramp_rate_one_forecast_horizon_minutes(
                session=session,
                model_name=model_name,
                datetime_interval=datetime_interval,
                forecast_horizon_minutes=forecast_horizon_hour*60,
                ramp_rate_minutes=60,
            )
