""" Function to make RMSE """
import logging
import os
from typing import Optional, Union

from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.models import ForecastValueLatestSQL, ForecastValueSevenDaysSQL, Metric, MLModelSQL
from nowcasting_datamodel.models.gsp import GSPYieldSQL, LocationSQL
from nowcasting_datamodel.models.metric import DatetimeInterval
from nowcasting_datamodel.read.read import get_location
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import func

from nowcasting_metrics.metrics.utils import (
    default_max_forecast_horizon_minutes,
    filter_query_on_datetime_interval,
    make_forecast_sub_query,
    make_gsp_sub_query,
    make_pvlive_subquery,
)
from nowcasting_metrics.utils import save_metric_value_to_database

logger = logging.getLogger(__name__)

use_pvnet_gsp_sum = os.getenv("USE_PVNET_GSP_SUM", "False").lower() == "true"


latest_rmse = Metric(
    name="Daily Latest RMSE",
    description="This metric calculates the RMSE for the latest OCF forecast "
    "and compares with the PVLive values. The data is from one day",
)

latest_rmse_with_adjuster = Metric(
    name="Daily Latest RMSE with adjuster",
    description="This metric calculates the RMSE for the latest OCF forecast "
    "and compares with the PVLive values. The data is from one day. "
    "We use the adjuster values.",
)

rmse_all_gsps = Metric(
    name="Daily Latest RMSE All GSPs",
    description="This metric calculates the RMSE for the latest OCF forecast "
    "and compares with the PVLive values. The data is from one day. "
    "This is for all GSPs (not the national)",
)

pvlive_rmse = Metric(
    name="PVLive RMSE",
    description="This metric calculates the RMSE for the initial estimate by "
    "PVLive and the updated estimate. The data is from one day for each GSP.",
)


def make_pvlive_rmse(
    session: Session, datetime_interval: DatetimeInterval, gsp_id: int
) -> (int, int):
    """
    Calculate RMSE for the PV Live initial and updates estimate

    :param session: database sessions
    :param datetime_interval: datetime interval
    :param gsp_id: the gsp id
    :return:  1. the RMSE, 2. the number of data points
    """

    sub_query_day_after = make_pvlive_subquery(
        session=session, datetime_interval=datetime_interval, gsp_id=gsp_id, regime="day-after"
    ).subquery()

    sub_query_in_day = make_pvlive_subquery(
        session=session, datetime_interval=datetime_interval, gsp_id=gsp_id, regime="in-day"
    ).subquery()

    query = session.query(
        func.sqrt(
            func.avg(
                func.pow(
                    sub_query_day_after.c.solar_generation_kw / 1000
                    - sub_query_in_day.c.solar_generation_kw / 1000,
                    2,
                )
            )
        ),
        func.count(sub_query_day_after.c.datetime_utc),
    )

    query = query.join(
        sub_query_in_day, sub_query_day_after.c.datetime_utc == sub_query_in_day.c.datetime_utc
    )
    results = query.all()

    number_of_data_points = results[0][1]
    value = results[0][0]

    logger.debug(
        f"Found PVlive RMSE of {value} from {number_of_data_points} " f"data points for {gsp_id=}."
    )

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=pvlive_rmse,
        location=get_location(gsp_id=gsp_id, session=session),
    )

    return value, number_of_data_points


def make_rmse_one_gsp_with_forecast_horizon(
    session: Session,
    datetime_interval: DatetimeInterval,
    gsp_id: int,
    forecast_horizon_minutes: int,
    use_adjuster: Optional[bool] = False,
    metric: Optional[Metric] = latest_rmse,
    model_name: Optional[str] = None,
) -> (int, int):
    """
    Calculate the RMSE for one GSP for a forecast horizon, and save to database

    :param session: database session
    :param datetime_interval: datetime interbal
    :param gsp_id: the gsp id
    :param forecast_horizon_minutes: the forecast horizon ie. Use results from forecast that are
        made 60 minutes before target time
    :param model_name: the model name of the forecast. This is optional.
    :return: 1. the MAE, 2. the number of data points
    """

    logger.debug(f'Calculating {metric.name} for {gsp_id=} and {forecast_horizon_minutes=} '
                 f'with {use_adjuster=} for {model_name=}')

    sub_query_gsp = make_gsp_sub_query(datetime_interval, gsp_id, session)
    sub_query_forecast = make_forecast_sub_query(
        datetime_interval, forecast_horizon_minutes, gsp_id, session, model_name=model_name
    )

    # make full query
    query = make_rmse_query(session, model=ForecastValueSevenDaysSQL, use_adjuster=use_adjuster)

    query = query.filter(ForecastValueSevenDaysSQL.uuid.in_(sub_query_forecast))
    query = query.filter(GSPYieldSQL.id.in_(sub_query_gsp))
    query = query.filter(GSPYieldSQL.datetime_utc == ForecastValueSevenDaysSQL.target_time)
    results = query.all()

    number_of_data_points = results[0][1]
    value = results[0][0]

    logger.debug(
        f"Found RMSE of {value} from {number_of_data_points} "
        f"data points for forecast horizon {forecast_horizon_minutes} for {gsp_id=}."
    )

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=metric,
        location=get_location(gsp_id=gsp_id, session=session),
        forecast_horizon_minutes=forecast_horizon_minutes,
        model_name=model_name
    )

    return value, number_of_data_points


def make_rmse_one_gsp(
    session: Session,
    datetime_interval: DatetimeInterval,
    gsp_id: int,
    use_adjuster: Optional[bool] = False,
    metric: Optional[Metric] = latest_rmse,
    model_name: Optional[str] = None,
) -> ():
    """
    Calculate the RMSE for one GSP, and save to database

    :param session: database session
    :param datetime_interval: datetime interbal
    :param gsp_id: the gsp id
    :param use_adjuster: whether to use the adjuster or not
    :param metric: the metric to use
    :param model_name: the model name of the forecast. This is optional.
    :return: 1. the MAE, 2. the number of data points
    """

    logger.debug(
        f"Calculating RMSE for last forecast for {gsp_id=} for {model_name=}"
        f"for start={datetime_interval.end_datetime_utc} "
        f"and end-{datetime_interval.end_datetime_utc}"
    )

    query = make_rmse_query(session, use_adjuster=use_adjuster, model_name=model_name)

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
    value = results[0][0]

    logger.debug(f"Found RMSE of {value} from {number_of_data_points} data points.")

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=metric,
        location=get_location(gsp_id=gsp_id, session=session),
        model_name=model_name
    )

    return value, number_of_data_points


def make_rmse_all_gsp(session: Session, datetime_interval: DatetimeInterval, model_name: Optional[str] = None,):
    """
    Calculate the RMSE for all GSP (not national), and save to database

    :param session: database session
    :param datetime_interval: datetime interbal
    :param model_name: the model name of the forecast. This is optional.
    :return: 1. the MAE, 2. the number of data points
    """

    logger.debug(
        f"Calculating RMSE for last forecast for all gsps (not national) for {model_name=}"
        f"for start={datetime_interval.end_datetime_utc} "
        f"and end-{datetime_interval.end_datetime_utc}"
    )

    query = make_rmse_query(session, model_name=model_name)

    # filter on gsp
    query = query.filter()

    # filter on target time
    query = query.join(GSPYieldSQL.location)
    query = query.filter(LocationSQL.gsp_id != 0)
    query = query.filter(ForecastValueLatestSQL.gsp_id != 0)
    query = query.filter(ForecastValueLatestSQL.gsp_id == LocationSQL.gsp_id)

    # only include non nan values
    query = query.filter(GSPYieldSQL.solar_generation_kw + 1 > GSPYieldSQL.solar_generation_kw)

    # join target time and yield
    query = filter_query_on_datetime_interval(datetime_interval, query)

    # filter on gsp regime
    query = query.filter(GSPYieldSQL.regime == "day-after")

    results = query.all()

    number_of_data_points = results[0][1]
    value = results[0][0]

    logger.debug(f"Found RMSE of {value} from {number_of_data_points} data points.")

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=rmse_all_gsps,
        model_name=model_name
    )

    return value, number_of_data_points


def make_rmse_query(
    session,
    model: Union[ForecastValueSevenDaysSQL, ForecastValueLatestSQL] = ForecastValueLatestSQL,
    use_adjuster: bool = False,
    model_name: Optional[str] = None,
):
    """
    Make rmse query

    :param session: database sessions
    :param model: the model to use
    :param use_adjuster: whether to use the adjuster or not
    :return: query
    """

    forecast = model.expected_power_generation_megawatts
    if use_adjuster:
        forecast = forecast - model.adjust_mw

    query = session.query(
        func.sqrt(func.avg(func.pow(forecast - GSPYieldSQL.solar_generation_kw / 1000, 2))),
        func.count(model.expected_power_generation_megawatts),
    )

    if model_name is not None:
        query = query.join(MLModelSQL, model.model_id == MLModelSQL.id)
        query = query.filter(MLModelSQL.name == model_name)

    return query


def make_rmse(
    session: Session,
    datetime_interval: DatetimeInterval,
    n_gsps: Optional[int] = N_GSP,
    max_forecast_horizon_minutes: Optional[dict] = None,
):
    """
    Calculate RMSE for all GSPs

    :param session: database session
    :param datetime_interval: datetime interval
    :param n_gsps: The number of gsps (+1 for national)
    :param: max_forecast_horizon_minutes.
        The maximum forecast horizon we should look at, default is set below
    """

    if max_forecast_horizon_minutes is None:
        max_forecast_horizon_minutes = default_max_forecast_horizon_minutes

    models = ["cnn", "pvnet_v2", "National_xg", "pvnet_day_ahead"]
    if use_pvnet_gsp_sum:
        models.append("pvnet_gsp_sum")

    # national
    for model_name in ["cnn", "pvnet_v2", "National_xg", "pvnet_day_ahead"]:
        make_rmse_one_gsp(
            session=session,
            datetime_interval=datetime_interval,
            gsp_id=0,
            use_adjuster=True,
            metric=latest_rmse_with_adjuster,
            model_name=model_name
        )

        make_rmse_all_gsp(session=session, datetime_interval=datetime_interval, model_name=model_name)

        # loop over forecast horizons
        for forecast_horizon_minutes in range(0, max_forecast_horizon_minutes[model_name], 30):
            make_rmse_one_gsp_with_forecast_horizon(
                session=session,
                datetime_interval=datetime_interval,
                gsp_id=0,
                forecast_horizon_minutes=forecast_horizon_minutes,
                model_name=model_name,
            )

            make_rmse_one_gsp_with_forecast_horizon(
                session=session,
                datetime_interval=datetime_interval,
                gsp_id=0,
                forecast_horizon_minutes=forecast_horizon_minutes,
                use_adjuster=True,
                metric=latest_rmse_with_adjuster,
                model_name=model_name,
            )

    # loop over gsps
    for model_name in ["cnn", "pvnet_v2", "pvnet_day_ahead"]:
        for gps_id in range(0, n_gsps + 1):
            make_rmse_one_gsp(session=session, datetime_interval=datetime_interval, gsp_id=gps_id, model_name=model_name)

    # pvlive
    for gps_id in range(0, n_gsps + 1):
        make_pvlive_rmse(session=session, datetime_interval=datetime_interval, gsp_id=gps_id)
