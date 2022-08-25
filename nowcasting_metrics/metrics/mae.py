from nowcasting_datamodel.models import Metric
from nowcasting_datamodel.models.metric import DatetimeInterval
from nowcasting_metrics.utils import save_metric_value_to_database
from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.read.read import get_location


latest_mae = Metric(
    name="Daily Latest MAE",
    description="This metric calculates the MAE for the latest OCF forecast "
    "and compares with the PVLive values. The data is from one day",
)


def make_mae_one_gsp(session, datetime_interval: DatetimeInterval, gsp_id: int):

    # TODO Issue https://github.com/openclimatefix/nowasting_metrics/issues/2

    number_of_data_points = 1
    value = 1

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=latest_mae,
        location=get_location(gsp_id=gsp_id, session=session),
    )

    return value, number_of_data_points


def make_mae(session, datetime_interval: DatetimeInterval):

    # for the latest forecast made

    # loop over gsps
    for gps_id in range(0, N_GSP + 1):
        make_mae_one_gsp(session=session, datetime_interval=datetime_interval, gsp_id=gps_id)
    # TODO add logging
