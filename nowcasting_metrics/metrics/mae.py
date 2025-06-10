""" Function to make MAE

1. Calculate the Mean Absolute Error (MAE) for different forecast horizons. This is done for National
2. Calculate the MAE for PVLive initial and updated estimates.
3. Calculate the MAE for each GSPs (not national) for latest forecasts
4. Calculate the MAE for all GSPs (not national) for all forecasts

1. has been optimized by only loading the data once, 2.-4. load the data from the database each time.

"""
import logging
import os
from datetime import timezone
from typing import Optional, Union

import numpy as np
import pandas as pd
from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.models import ForecastValueLatestSQL, ForecastValueSevenDaysSQL, Metric
from nowcasting_datamodel.models import (
    MLModelSQL,
)
from nowcasting_datamodel.models.gsp import GSPYieldSQL
from nowcasting_datamodel.models.gsp import LocationSQL
from nowcasting_datamodel.models.metric import DatetimeInterval
from nowcasting_datamodel.read.read import get_location
from nowcasting_datamodel.read.read_models import get_models
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import func

from nowcasting_metrics.metrics.utils import (
    default_gsp_models,
    filter_query_on_datetime_interval,
    get_forecast_range,
    make_pvlive_subquery,
)
from nowcasting_metrics.metrics.utils import (
    default_max_forecast_horizon_minutes,
)
from nowcasting_metrics.utils import save_metric_value_to_database

logger = logging.getLogger(__name__)

use_pvnet_gsp_sum = os.getenv("USE_PVNET_GSP_SUM", "False").lower() == "true"

latest_mae = Metric(
    name="Daily Latest MAE",
    description="This metric calculates the MAE for the latest OCF forecast "
    "and compares with the PVLive values. The data is from one day",
)

latest_mae_with_adjuster = Metric(
    name="Daily Latest MAE with adjuster",
    description="This metric calculates the MAE for the latest OCF forecast "
    "and compares with the PVLive values. The data is from one day. "
    "The value include the adjuster results",
)

mae_all_gsps = Metric(
    name="Daily Latest MAE All GSPs",
    description="This metric calculates the MAE for the latest OCF forecast "
    "and compares with the PVLive values. The data is from one day. "
    "This is for all GSPs (not the national)",
)

pvlive_mae = Metric(
    name="PVLive MAE",
    description="This metric calculates the MAE for the initial estimate by "
    "PVLive and the updated estimate. The data is from one day for each GSP.",
)


def make_pvlive_mae(
    session: Session, datetime_interval: DatetimeInterval, gsp_id: int
) -> (int, int):
    """
    Calculate MAE for the PV Live initial and updates estimate

    :param session: database sessions
    :param datetime_interval: datetime interval
    :param gsp_id: the gsp id
    :return:  1. the MAE, 2. the number of data points
    """

    sub_query_day_after = make_pvlive_subquery(
        session=session, datetime_interval=datetime_interval, gsp_id=gsp_id, regime="day-after"
    ).subquery()

    sub_query_in_day = make_pvlive_subquery(
        session=session, datetime_interval=datetime_interval, gsp_id=gsp_id, regime="in-day"
    ).subquery()

    query = session.query(
        func.avg(
            func.abs(
                sub_query_day_after.c.solar_generation_kw / 1000
                - sub_query_in_day.c.solar_generation_kw / 1000
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
        f"Found PVlive MAE of {value} from {number_of_data_points} " f"data points for {gsp_id=}."
    )

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=pvlive_mae,
        location=get_location(gsp_id=gsp_id, session=session),
    )

    return value, number_of_data_points


def make_mae_values(
    session: Session,
    datetime_interval: DatetimeInterval,
    forecast_values: pd.DataFrame,
    gsp_yields: pd.DataFrame,
    metric: Optional[Metric] = latest_mae,
    model_name: Optional[str] = None,
    use_adjuster: bool = True,
    forecast_horizon_minutes: Optional[int] = None
) -> (int, int):
    """
    Calculate the MAE for one GSP, and save to database

    :param session: database session
    :param datetime_interval: datetime interval
    :param use_adjuster: option to use the adjuster or not.
    :param metric: the metric to use
    :param model_name: the model name of the forecast. This is optional.
    :param use_adjuster: option to use the adjuster or not.
    :param forecast_horizon_minutes: the forecast horizon ie. Use results from forecast that are
        made 60 minutes before target time
    :param forecast_values: the forecast values for the last seven days
    :param gsp_yields: the GSP yields for the last seven days
    :return: 1. the MAE, 2. MAE with adjuster, 3. the number of data points
    """

    logger.debug(
        f"Calculating MAE for last forecast for {model_name=} for"
        f"start={datetime_interval.start_datetime_utc} "
        f"and end-{datetime_interval.end_datetime_utc}"
        f" and {forecast_horizon_minutes=}"
    )

    if len(forecast_values) == 0:
        logger.warning(
            f"Forecast values are empty for {model_name=}"
        )
        return ()

    start_datetime_utc = datetime_interval.start_datetime_utc.replace(tzinfo=timezone.utc)
    end_datetime_utc = datetime_interval.end_datetime_utc.replace(tzinfo=timezone.utc)

    gsp_yields = gsp_yields[gsp_yields.index >= start_datetime_utc]
    gsp_yields = gsp_yields[gsp_yields.index <= end_datetime_utc]

    forecast_values = forecast_values.copy()
    forecast_values = forecast_values[forecast_values.index >= start_datetime_utc]
    forecast_values = forecast_values[forecast_values.index <= end_datetime_utc]
    if forecast_horizon_minutes is not None:
        forecast_values = forecast_values[
            forecast_values.index
            > forecast_values.created_utc + pd.Timedelta(minutes=forecast_horizon_minutes)
            ]

    # take first forecast value for each index, this is becasue they are order by created_utc descing.
    # this means we get the latest forecast for a given forecast_horizon_minutes
    forecast_values = forecast_values.groupby(forecast_values.index).first()
    forecast_values = forecast_values.join(gsp_yields, how="inner", rsuffix="_forecast")

    # calculate the MAE
    forecast_values["error"] = (
            forecast_values.expected_power_generation_megawatts
            - forecast_values.solar_generation_kw / 1000
    ).abs()

    # calculate the MAE
    forecast_values["error_adjuster"] = (
            forecast_values.expected_power_generation_megawatts
            - forecast_values.adjust_mw
            - forecast_values.solar_generation_kw / 1000
    ).abs()

    value = float(forecast_values["error"].mean())
    value_adjuster = float(forecast_values["error_adjuster"].mean())
    number_of_data_points = int(forecast_values["error"].count())
    if np.isnan(value):
        value = None
    if np.isnan(value_adjuster):
        value_adjuster = None

    logger.info(value)
    logger.info(f"value_adjuster: {value_adjuster}")
    logger.info(f"number_of_data_points: {number_of_data_points}")

    logger.debug(f"Found MAE of {value} from {number_of_data_points} data points.")

    location = get_location(gsp_id=0, session=session)

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=metric,
        location=location,
        model_name=model_name,
        forecast_horizon_minutes=forecast_horizon_minutes
    )

    if use_adjuster:
        save_metric_value_to_database(
            session=session,
            value=value_adjuster,
            number_of_data_points=number_of_data_points,
            datetime_interval=datetime_interval,
            metric=latest_mae_with_adjuster,
            location=location,
            model_name=model_name,
            forecast_horizon_minutes=forecast_horizon_minutes
        )

    return value, value_adjuster, number_of_data_points

def make_mae_one_gsp(
    session: Session,
    datetime_interval: DatetimeInterval,
    gsp_id: int,
    metric: Optional[Metric] = latest_mae,
    model_name: Optional[str] = None,
    use_adjuster: bool = True,
) -> (int, int):
    """
    Calculate the MAE for one GSP, and save to database

    :param session: database session
    :param datetime_interval: datetime interval
    :param gsp_id: the gsp id
    :param use_adjuster: option to use the adjuster or not.
    :param metric: the metric to use
    :param model_name: the model name of the forecast. This is optional.
    :param use_adjuster: option to use the adjuster or not.
    :return: 1. the MAE, 2. MAE with adjuster, 3. the number of data points
    """

    logger.debug(
        f"Calculating MAE for last forecast for {gsp_id=} for {model_name=} for"
        f"start={datetime_interval.start_datetime_utc} "
        f"and end-{datetime_interval.end_datetime_utc}"
    )

    query = make_mae_query(session, model_name=model_name)

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

    number_of_data_points = results[0][2]
    value = results[0][0]
    value_adjuster = results[0][1]

    logger.debug(f"Found MAE of {value} from {number_of_data_points} data points.")

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=metric,
        location=get_location(gsp_id=gsp_id, session=session),
        model_name=model_name,
    )

    if use_adjuster:
        save_metric_value_to_database(
            session=session,
            value=value_adjuster,
            number_of_data_points=number_of_data_points,
            datetime_interval=datetime_interval,
            metric=latest_mae_with_adjuster,
            location=get_location(gsp_id=gsp_id, session=session),
            model_name=model_name,
        )

    return value, value_adjuster, number_of_data_points


def make_mae_all_gsp(
    session: Session,
    datetime_interval: DatetimeInterval,
    model_name: Optional[str] = None,
) -> (int, int):
    """
    Calculate the MAE for all GSPs (not national), and save to database

    :param session: database session
    :param datetime_interval: datetime interval
    :param model_name: the model name of the forecast. This is optional.
    :return: 1. the MAE, 2. the number of data points
    """

    logger.debug(
        f"Calculating MAE for last forecast for all gsps (not national) for {model_name=} for"
        f"start={datetime_interval.end_datetime_utc} "
        f"and end-{datetime_interval.end_datetime_utc}"
    )

    query = make_mae_query(session, model_name=model_name)

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

    number_of_data_points = results[0][2]
    value = results[0][0]

    logger.debug(
        f"Found MAE of {value} from {number_of_data_points} data points, "
        f"for all gsps (not national)"
    )

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=mae_all_gsps,
        location=None,
        model_name=model_name,
    )

    return value, number_of_data_points


def make_mae_query(
    session,
    model: Union[ForecastValueSevenDaysSQL, ForecastValueLatestSQL] = ForecastValueLatestSQL,
    model_name: Optional[str] = None,
):
    """
    Make MAE query

    :param session: database sessions
    :param model: either ForecastValueSQL or ForecastValueLatestSQL
    :param model_name: the model name of the forecast. This is optional.
    :return: query
    """
    forecast = model.expected_power_generation_megawatts

    query = session.query(
        func.avg(func.abs(forecast - GSPYieldSQL.solar_generation_kw / 1000)),
        func.avg(func.abs(forecast - model.adjust_mw - GSPYieldSQL.solar_generation_kw / 1000)),
        func.count(model.expected_power_generation_megawatts),
    )

    if model_name is not None:
        query = query.join(MLModelSQL, model.model_id == MLModelSQL.id)
        query = query.filter(MLModelSQL.name == model_name)

    return query


def make_mae(
    session: Session,
    datetime_interval: DatetimeInterval,
    all_forecast_values: dict,
    gsp_yields: pd.DataFrame,
    n_gsps: Optional[int] = N_GSP,
    max_forecast_horizon_minutes: Optional[dict] = None,
):
    """
    Calculate MAE for all GSPs

    :param session: database session
    :param datetime_interval: datetime interval
    :param n_gsps: The number of Gsps to loop over. Default is N_GSP. (+1 for national)
    :param max_forecast_horizon_minutes.
        The maximum forecast horizon we should look at, default is set below
    :param all_forecast_values: all forecast values for the last seven days
    :param gsp_yields: the GSP yields for the last seven days
    """

    if max_forecast_horizon_minutes is None:
        max_forecast_horizon_minutes = default_max_forecast_horizon_minutes

    # this gets all the models used in the last week
    models = get_models(
        session=session,
        with_forecasts=True,
        forecast_created_utc=datetime_interval.start_datetime_utc,
    )
    models = [model.name for model in models]
    if use_pvnet_gsp_sum and "pvnet_gsp_sum" not in models:
        models.append("pvnet_gsp_sum")

    # make sure models in max_forecast_horizon_minutes
    for model in models:
        if model not in max_forecast_horizon_minutes:
            max_forecast_horizon_minutes[model] = 480

    # 1. Calculate the MAE for the forecast values for each model
    for model_name in models:

        if model_name not in all_forecast_values:
            logger.warning(f"No forecast values for model {model_name} for me, skipping...")
            continue

        forecast_values_df = all_forecast_values[model_name]

        # loop over forecast horizons
        # we want to run the MAE for no forecast horizon as well as each forecast horizon
        for forecast_horizon_minutes in [None] + list(get_forecast_range(max_forecast_horizon_minutes[model_name])):
            make_mae_values(
                session=session,
                datetime_interval=datetime_interval,
                forecast_horizon_minutes=forecast_horizon_minutes,
                model_name=model_name,
                forecast_values=forecast_values_df,
                gsp_yields=gsp_yields
            )


    # Below are metrics made by querying the database directly, rather than using forecast values
    # This can be a improvement in the future

    # 2. pvlive
    for gps_id in range(0, n_gsps + 1):
        make_pvlive_mae(session=session, datetime_interval=datetime_interval, gsp_id=gps_id)

    for model_name in default_gsp_models:
        # 3. for ecah gsps
        for gps_id in range(1, n_gsps + 1):
            make_mae_one_gsp(
                session=session,
                datetime_interval=datetime_interval,
                gsp_id=gps_id,
                model_name=model_name,
                use_adjuster=False,
            )

        # 4. all gsps (not national)
        make_mae_all_gsp(
            session=session, datetime_interval=datetime_interval, model_name=model_name
        )
