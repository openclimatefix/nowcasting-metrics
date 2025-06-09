import logging
from datetime import timezone

import pandas as pd
from nowcasting_datamodel.models import (
    DatetimeInterval,
    Metric,
)
from nowcasting_datamodel.read.read import get_location
from sqlalchemy.orm.session import Session

from nowcasting_metrics.metrics.utils import default_national_models
from nowcasting_metrics.utils import save_metric_value_to_database

logger = logging.getLogger(__name__)

ramp_rate = Metric(
    name="Ramp rate MAE",
    description="This metric calculates the MAE ramp rate for the latest OCF forecast "
    "and compares with the PVLive values. Ramp rate is defined as from one forecast run "
    "((pred_{t+1 hour} - pred_{t}) - (true_{t+1 hour} - true_{t})) ."
    "We take the absolute value of the ramp rate and calculate the mean ",
)


def make_ramp_rate_one_forecast_horizon_minutes(
    session,
    model_name: str,
    datetime_interval: DatetimeInterval,
    forecast_horizon_minutes: int,
    ramp_rate_minutes: int,
    forecast_values: pd.DataFrame,
    gsp_yields: pd.DataFrame,
):
    """Calculate one ramp rate metric for a given forecast horizon"""

    start_datetime_utc = datetime_interval.start_datetime_utc.replace(tzinfo=timezone.utc)
    end_datetime_utc = datetime_interval.end_datetime_utc.replace(tzinfo=timezone.utc)

    if len(forecast_values) == 0:
        logger.warning(
            f"Forecast values are empty for {model_name=}"
        )
        return ()

    gsp_yields = gsp_yields[gsp_yields.index >= start_datetime_utc]
    gsp_yields = gsp_yields[gsp_yields.index <= end_datetime_utc]

    forecast_values = forecast_values.copy()
    forecast_values = forecast_values[forecast_values.index >= start_datetime_utc]
    forecast_values = forecast_values[forecast_values.index <= end_datetime_utc]

    # using ramp_rate_minutes, lets shift the index back by ramp_rate_minutes, and then join
    forecast_values_ramp = forecast_values.copy()
    forecast_values_ramp.index = forecast_values_ramp.index - pd.Timedelta(minutes=ramp_rate_minutes)
    forecast_values = forecast_values.join(
        forecast_values_ramp,
        how="inner",
        rsuffix="_ramp",
    )
    # and lets do the same for gsp_yields
    gsp_yields_ramp = gsp_yields.copy()
    gsp_yields_ramp.index = gsp_yields_ramp.index - pd.Timedelta(minutes=ramp_rate_minutes)
    gsp_yields = gsp_yields.join(
        gsp_yields_ramp,
        how="inner",
        rsuffix="_ramp",
    )

    if forecast_horizon_minutes is not None:
        forecast_values = forecast_values[
            forecast_values.index
            > forecast_values.created_utc + pd.Timedelta(minutes=forecast_horizon_minutes)
            ]

    # take first forecast value for each index, this is becasue they are order by created_utc descing.
    # this means we get the latest forecast for a given forecast_horizon_minutes
    forecast_values = forecast_values.groupby(forecast_values.index).first()
    forecast_values = forecast_values.join(gsp_yields, how="inner", rsuffix="_forecast")

    # calculate the ramp rate
    forecast_values["ramp_rate"] = (
            forecast_values.expected_power_generation_megawatts
            - forecast_values.expected_power_generation_megawatts_ramp
            - forecast_values.solar_generation_kw / 1000
            + forecast_values.solar_generation_kw_ramp / 1000
    ).abs()

    value = forecast_values["ramp_rate"].mean()
    number_of_data_points = len(forecast_values)

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
    all_forecast_values: dict,
    gsp_yields: pd.DataFrame,
):
    """
    Make ramp rate for all models and forecast horizons

    :param session: database session
    :param datetime_interval: datetime interval
    :param all_forecast_values: all forecast values for the last seven days
    :param gsp_yields: the GSP yields for the last seven days
    :return: None
    """
    forecast_horizon_hours = [0, 1, 2]
    for forecast_horizon_hour in forecast_horizon_hours:
        for model_name in default_national_models:

            if model_name not in all_forecast_values:
                logger.warning(f"No forecast values for model {model_name} for me, skipping...")
                continue

            forecast_values_df = all_forecast_values[model_name]

            make_ramp_rate_one_forecast_horizon_minutes(
                session=session,
                model_name=model_name,
                datetime_interval=datetime_interval,
                forecast_horizon_minutes=forecast_horizon_hour*60,
                ramp_rate_minutes=60,
                forecast_values=forecast_values_df,
                gsp_yields=gsp_yields,
            )
