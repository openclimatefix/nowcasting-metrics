""" General metric functions """

from nowcasting_datamodel.read.read_metric import get_metric

from nowcasting_metrics.metrics.mae import latest_mae, mae_all_gsps, pvlive_mae
from nowcasting_metrics.metrics.me import me_hh
from nowcasting_metrics.metrics.rmse import latest_rmse, pvlive_rmse, rmse_all_gsps
from nowcasting_metrics.metrics.ramp_rate import ramp_rate

all_metrics = [latest_mae, mae_all_gsps, latest_rmse, rmse_all_gsps, pvlive_mae, pvlive_rmse, me_hh, ramp_rate]


def check_metrics_in_database(session):
    """
    Check metrics are in the database, and update description

    :param session: the database session
    """

    # Make sure they are in the database
    for metric in all_metrics:
        metric_orm = get_metric(session=session, name=metric.name)

        if metric_orm.description != metric.description:
            metric_orm.description = metric.description

    return all_metrics
