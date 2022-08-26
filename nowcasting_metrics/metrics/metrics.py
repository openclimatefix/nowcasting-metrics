from nowcasting_datamodel.read.read_metric import get_metric

from nowcasting_metrics.metrics.mae import latest_mae, mae_forecast_horizon
from nowcasting_metrics.metrics.rmse import latest_rmse, rmse_forecast_horizon
from nowcasting_metrics.metrics.utils import add_forecast_horizon_to_metric
from nowcasting_metrics.utils import get_all_forecast_horizons

all_metrics = [latest_mae, latest_rmse]
forecast_horizon_metrics = [mae_forecast_horizon, rmse_forecast_horizon]


def check_metrics_in_database(session):
    """
    Check all metrics are in the database

    :param session: database session
    """

    all_forecast_horizon_metrics = []
    for metric in forecast_horizon_metrics:

        # from 30 mins to 8 hours
        for forecast_horizon_minutes in get_all_forecast_horizons():
            metric = add_forecast_horizon_to_metric(
                metric=metric, forecast_horizon_minutes=forecast_horizon_minutes
            )
            all_forecast_horizon_metrics.append(metric)

    # Loops over latest metrics
    for metric in all_metrics + all_forecast_horizon_metrics:
        metric_orm = get_metric(session=session, name=metric.name)

        if metric_orm.description != metric.description:
            metric_orm.description = metric.description

    return all_metrics + all_forecast_horizon_metrics
