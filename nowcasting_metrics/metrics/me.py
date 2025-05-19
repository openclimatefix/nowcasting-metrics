""" Function to make MAE """
import logging
from typing import Optional, Union

from datetime import timezone

import pandas as pd
from nowcasting_datamodel.models import ForecastValueLatestSQL, ForecastValueSevenDaysSQL, Metric
from nowcasting_datamodel.models.gsp import GSPYieldSQL
from nowcasting_datamodel.models.metric import DatetimeInterval
from nowcasting_datamodel.read.read import get_location
from sqlalchemy import Time, cast
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import func

from nowcasting_metrics.metrics.utils import (
    default_max_forecast_horizon_minutes,
    default_national_models,
)
from nowcasting_metrics.utils import save_metric_value_to_database
from nowcasting_datamodel.read.read_metric import get_datetime_interval, get_metric

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
    forecast_values: pd.DataFrame,
    gsp_yields: pd.DataFrame,
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
    :param forecast_values: the forecast values
    :param gsp_yields: the gsp yields
    :param model_name: the model name of the forecast. This is optional.
    :param save_to_database: if True, save the results to the database
    :return: 1. the MAE, 2. the number of data points
    """

    if len(forecast_values) == 0:
        logger.warning(
            f"Forecast values are empty for {model_name=} and {forecast_horizon_minutes=}"
        )
        return []

    start_datetime_utc = datetime_interval.start_datetime_utc.replace(tzinfo=timezone.utc)
    end_datetime_utc = datetime_interval.end_datetime_utc.replace(tzinfo=timezone.utc)

    gsp_yields = gsp_yields[gsp_yields.index >= start_datetime_utc]
    gsp_yields = gsp_yields[gsp_yields.index <= end_datetime_utc]

    forecast_values = forecast_values.copy()
    forecast_values = forecast_values[forecast_values.index >= start_datetime_utc]
    forecast_values = forecast_values[forecast_values.index <= end_datetime_utc]
    forecast_values = forecast_values[
        forecast_values.index
        > forecast_values.created_utc + pd.Timedelta(minutes=forecast_horizon_minutes)
    ]

    # take first forecast value for each index, this is becasue they are order by created_utc descing.
    # this means we get the latest forecast for a given forecast_horizon_minutes
    forecast_values = forecast_values.groupby(forecast_values.index).first()

    # join together with gsp_yields
    forecast_values = forecast_values.join(gsp_yields, how="inner", rsuffix="_forecast")

    # create new columns of time
    forecast_values["time_of_day"] = forecast_values.index.time

    # calculate the ME
    forecast_values["error"] = (
        forecast_values.expected_power_generation_megawatts
        - forecast_values.solar_generation_kw / 1000
    )

    # group by time_of_day
    results_df = forecast_values[["error", "time_of_day"]].groupby("time_of_day").mean()
    results_count = forecast_values[["error", "time_of_day"]].groupby("time_of_day").count()
    # join
    results_df = results_df.join(results_count, on="time_of_day", rsuffix="_count")

    # rename error to mae
    results_df = results_df.rename(columns={"error": "mae"})

    location = get_location(gsp_id=gsp_id, session=session)
    metric_sql = get_metric(session=session, name=me_hh.name)
    datetime_interval_sql = get_datetime_interval(
        session=session,
        start_datetime_utc=datetime_interval.start_datetime_utc,
        end_datetime_utc=datetime_interval.end_datetime_utc,
    )

    results = []
    for i, result in results_df.iterrows():
        number_of_data_points = int(result.error_count)
        value = float(result.mae)
        time_of_day = result.name
        results.append([value, number_of_data_points, time_of_day])

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
                datetime_interval=datetime_interval_sql,
                time_of_day=time_of_day,
                metric=metric_sql,
                location=location,
                forecast_horizon_minutes=forecast_horizon_minutes,
                model_name=model_name,
            )

    session.commit()

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
    all_forecast_values: dict,
    gsp_yields: pd.DataFrame,
    max_forecast_horizon_minutes: Optional[dict] = None,
):
    """
    Calculate MAE for all GSPs

    :param max_forecast_horizon_minutes:
    :param session: database session
    :param datetime_interval: datetime interval
    :param all_forecast_values: all forecast values for all models
        {model_name: forecast_values_df}
    :param gsp_yields: gsp yields
    :param: max_forecast_horizon_minutes.
        The maximum forecast horizon we should look at, default is 8 hours
    """

    if max_forecast_horizon_minutes is None:
        max_forecast_horizon_minutes = default_max_forecast_horizon_minutes

    # loop over forecast horizons
    for model_name in default_national_models:

        if model_name not in max_forecast_horizon_minutes:
            max_forecast_horizon_minutes[model_name] = default_max_forecast_horizon_minutes[
                model_name
            ]

        forecast_values_df = all_forecast_values[model_name]

        for forecast_horizon_minutes in range(0, max_forecast_horizon_minutes[model_name], 30):

            make_me_one_gsp_with_forecast_horizon_and_one_half_hour(
                session=session,
                datetime_interval=datetime_interval,
                gsp_id=0,
                forecast_horizon_minutes=forecast_horizon_minutes,
                model_name=model_name,
                forecast_values=forecast_values_df,
                gsp_yields=gsp_yields,
            )
