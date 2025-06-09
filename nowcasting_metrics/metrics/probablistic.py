""" Look at probabilistic metrics for nowcasting models

 We will look at
 - exceedence, the amount of times the values it over the plevel. We would expect 90% of valyes to be over the 10% plevel
 - pinball loss

 We will look at p levels 10 and 90
 for different forecast horizons

 """

from datetime import timezone
import logging
import pandas as pd
import numpy as np
from typing import Optional, Dict
from nowcasting_datamodel.models import (
    DatetimeInterval,
    Metric,
)
from nowcasting_datamodel.read.read import get_location
from sqlalchemy.orm.session import Session

from nowcasting_metrics.metrics.utils import (
    default_max_forecast_horizon_minutes,
    default_probabilistic_models,
    get_forecast_range,
)
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
    forecast_values: pd.DataFrame,
    gsp_yields: pd.DataFrame,
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
        f"for {model_name=}, {forecast_horizon_minutes=}, {p_level=}"
    )

    if len(forecast_values) == 0:
        logger.warning(
            f"Forecast values are empty for {model_name=}, "
            f"so cannot make pinball and exceedance metrics. "
        )
        return []

    # get a valye between 0 and 1
    tau = float(p_level) / 100

    start_datetime_utc = datetime_interval.start_datetime_utc.replace(tzinfo=timezone.utc)
    end_datetime_utc = datetime_interval.end_datetime_utc.replace(tzinfo=timezone.utc)

    gsp_yields = gsp_yields[gsp_yields.index >= start_datetime_utc]
    gsp_yields = gsp_yields[gsp_yields.index <= end_datetime_utc]

    forecast_values = forecast_values.copy()
    forecast_values = forecast_values[forecast_values.index >= start_datetime_utc]
    forecast_values = forecast_values[forecast_values.index <= end_datetime_utc]

    # filter for forecast horizon
    if forecast_horizon_minutes is not None:
        forecast_values = forecast_values[
            forecast_values.index
            > forecast_values.created_utc + pd.Timedelta(minutes=forecast_horizon_minutes)
            ]

    if len(forecast_values) == 0:
        logger.warning(
            f"Forecast values are empty for {model_name=} {forecast_horizon_minutes=}, "
            f"so cannot make pinball and exceedance metrics. "
        )
        return []

    # take first forecast value for each index, this is becasue they are order by created_utc descing.
    # this means we get the latest forecast for a given forecast_horizon_minutes
    forecast_values = forecast_values.groupby(forecast_values.index).first()
    forecast_values = forecast_values.join(gsp_yields, how="inner", rsuffix="_forecast")

    if len(forecast_values) == 0:
        logger.warning(
            f"No overlapping forecast valyes and gsp yields for {model_name=} {forecast_horizon_minutes=}, "
            f"so cannot make pinball and exceedance metrics. "
        )
        return []

    # change json column 'properties' to seperate columns
    forecast_values = forecast_values.copy()
    forecast_values = forecast_values.join(
        forecast_values.properties.apply(pd.Series),
        rsuffix="_properties",
    )

    forecast_values_under = forecast_values[
            forecast_values[p_level]
            <forecast_values.solar_generation_kw / 1000
    ]
    forecast_values_over = forecast_values[
            forecast_values[p_level]
            > forecast_values.solar_generation_kw / 1000
    ]
    under_average =  (forecast_values_under.solar_generation_kw / 1000 - forecast_values_under[p_level]).mean()
    over_average = (forecast_values_over[p_level] - forecast_values_over.solar_generation_kw / 1000).mean()

    if np.isnan(under_average):
        under_average = 0
    if np.isnan(over_average):
        over_average = 0

    number_of_data_points = len(forecast_values)
    number_of_data_points_under = len(forecast_values_under)
    number_of_data_points_over = len(forecast_values_over)

    fraction_under = number_of_data_points_under / number_of_data_points
    fraction_over = number_of_data_points_over / number_of_data_points

    # if there are no values under, then `results_under' is None, hence we need to check
    # similar fo values over
    pinball_value = 0
    if number_of_data_points_under is not None:
        pinball_value += (number_of_data_points_under * under_average) * (1 - tau)
    if number_of_data_points_over is not None:
        pinball_value += (number_of_data_points_over * over_average) * tau

    # divide by the number of data points to get average
    if number_of_data_points == 0:
        pinball_value = None
        exceedance_value = None
    else:
        pinball_value = pinball_value / number_of_data_points

        # to take account for night where the truth and prediction are both 0
        # for plevels above 50, we want to include these,
        # for plevels below 50, we want to exclude these
        # both results_under and results_over include these nighttime values
        if tau < 0.5:
            exceedance_value = 1 - fraction_under
        else:
            exceedance_value = fraction_over

        exceedance_value = float(exceedance_value)
        pinball_value = float(pinball_value)

    if np.isnan(exceedance_value):
        exceedance_value = None
    if np.isnan(pinball_value):
        pinball_value = None

    logger.debug(f"pinball: {pinball_value}")
    logger.debug(f"exceedance_value: {exceedance_value}")

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
        forecast_horizon_minutes=forecast_horizon_minutes,
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
        forecast_horizon_minutes=forecast_horizon_minutes,
    )

    return exceedance_value, pinball_value, number_of_data_points


def make_probabilistic(
    session: Session,
    datetime_interval: DatetimeInterval,
    all_forecast_values: dict,
    gsp_yields: pd.DataFrame,
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
        max_forecast_horizon_minutes = default_max_forecast_horizon_minutes

    for model_name in default_probabilistic_models:

        if model_name not in all_forecast_values:
            logger.warning(f"No forecast values for model {model_name} for pinball and exceedance, skipping...")
            continue

        for forecast_horizon_minute in get_forecast_range(max_forecast_horizon_minutes[model_name]):

            forecast_values_df = all_forecast_values[model_name]

            for p_level in ["10", "90"]:

                make_probabilistic_metrics_one_forecast_horizon_minutes(
                    session=session,
                    model_name=model_name,
                    datetime_interval=datetime_interval,
                    forecast_horizon_minutes=forecast_horizon_minute,
                    p_level=p_level,
                    forecast_values=forecast_values_df,
                    gsp_yields=gsp_yields,
                )
