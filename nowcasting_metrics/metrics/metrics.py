""" General metric functions """

from nowcasting_metrics.metrics.mae import latest_mae
from nowcasting_metrics.metrics.rmse import latest_rmse

from nowcasting_datamodel.read.read_metric import get_metric

all_metrics = [latest_mae, latest_rmse]


def check_metrics_in_database(session):

    # Make sure they are in the database
    for metric in all_metrics:
        metric_orm = get_metric(session=session, name=metric.name)

        if metric_orm.description != metric.description:
            metric_orm.description = metric.description

    return all_metrics
