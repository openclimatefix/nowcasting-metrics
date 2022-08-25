from nowcasting_datamodel.models import Metric
from nowcasting_datamodel.models.metric import DatetimeInterval
from nowcasting_metrics.utils import save_metric_value_to_database


latest_rmse = Metric(
    name="Daily Latest RMSE",
    description="This metric calculates the RMSE for the latest OCF forecast "
    "and compares with the PVLive values. The data is from one day",
)


def make_rmse(session, datetime_interval: DatetimeInterval):

    # For the latest forecast

    # TODO need to loop over GSPS
    # TODO add logging

    # TODO https://github.com/openclimatefix/nowasting_metrics/issues/3
    number_of_data_points = 1
    value = 1

    save_metric_value_to_database(
        session=session,
        value=value,
        number_of_data_points=number_of_data_points,
        datetime_interval=datetime_interval,
        metric=latest_rmse,
    )
