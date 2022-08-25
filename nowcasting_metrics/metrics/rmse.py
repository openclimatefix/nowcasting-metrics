from nowcasting_datamodel.models import Metric
from nowcasting_datamodel.models.metric import DatetimeInterval
from nowcasting_metrics.utils import save_metric_value_to_database
from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.read.read import get_location
from nowcasting_datamodel.models.models import ForecastValueLatestSQL
from nowcasting_datamodel.models.gsp import GSPYieldSQL, LocationSQL

from sqlalchemy.sql import func


latest_rmse = Metric(
    name="Daily Latest RMSE",
    description="This metric calculates the RMSE for the latest OCF forecast "
    "and compares with the PVLive values. The data is from one day",
)


def make_rmse_one_gsp(session, datetime_interval: DatetimeInterval, gsp_id: int):

    # TODO Issue https://github.com/openclimatefix/nowasting_metrics/issues/2

    query = session.query(
        func.avg(func.pow(
            ForecastValueLatestSQL.expected_power_generation_megawatts
            - GSPYieldSQL.solar_generation_kw / 1000
        ,2)),
        func.count(
            ForecastValueLatestSQL.expected_power_generation_megawatts)
    )

    # filter on gsp
    query = query.filter()

    # filter on target time
    query = query.join(GSPYieldSQL.location)
    query = query.filter(LocationSQL.gsp_id == gsp_id)
    query = query.filter(ForecastValueLatestSQL.gsp_id == gsp_id)

    # join target time and yield
    query = query.filter(ForecastValueLatestSQL.target_time > datetime_interval.start_datetime_utc)
    query = query.filter(ForecastValueLatestSQL.target_time <= datetime_interval.end_datetime_utc)
    query = query.filter(GSPYieldSQL.datetime_utc == ForecastValueLatestSQL.target_time)

    # filter on gsp regime
    query = query.filter(GSPYieldSQL.regime == "day-after")

    results = query.all()

    number_of_data_points = results[0][1]
    value = results[0][0]*0.5

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=latest_rmse,
        location=get_location(gsp_id=gsp_id, session=session),
    )

    return value, number_of_data_points


def make_rmse(session, datetime_interval: DatetimeInterval):

    # For the latest forecast

    # loop over gsps
    for gps_id in range(0, N_GSP + 1):
        make_rmse_one_gsp(session=session, datetime_interval=datetime_interval, gsp_id=gps_id)
    # TODO add logging

