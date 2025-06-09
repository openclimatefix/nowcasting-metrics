"""
This application will run metrics on the nowcasting forecast

1. Get all the metrics we want to use

2. Run each metric and save to database
The metrics are
A. MAE for each gsp from the last forecast
B. RMSE for each gsps form the last forecast

"""
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
import sentry_sdk

import click
from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models.metric import DatetimeInterval
from nowcasting_datamodel.read.read_metric import get_datetime_interval

import nowcasting_metrics
from nowcasting_metrics.database.forecast import get_all_forecast_values
from nowcasting_metrics.database.gsp_yield import get_gsp_yield
from nowcasting_metrics.metrics.mae import make_mae
from nowcasting_metrics.metrics.me import make_me
from nowcasting_metrics.metrics.metrics import check_metrics_in_database
from nowcasting_metrics.metrics.rmse import make_rmse
from nowcasting_metrics.metrics.ramp_rate import make_ramp_rate
from nowcasting_metrics.metrics.probablistic import make_probabilistic

logging.basicConfig(
    level=getattr(logging, os.getenv("LOGLEVEL", "DEBUG")),
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

#sentry
sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    environment=os.getenv("ENVIRONMENT", "local"),
    traces_sample_rate=1
)

sentry_sdk.set_tag("app_name", "nowcasting_metrics")
sentry_sdk.set_tag("version", nowcasting_metrics.__version__)

@click.command()
@click.option(
    "--db-url",
    default=None,
    envvar="DB_URL",
    help="The Database URL where forecasts will be saved",
    type=click.STRING,
)
@click.option(
    "--datetime-now",
    default=None,
    envvar="DATETIME_NOW",
    help="Which timestamp to use. Default is None, and now is used. "
    "Must be in the format YYYY-MM-DD",
    type=click.STRING,
)
@click.option(
    "--n-gsps",
    default=N_GSP,
    envvar="N_GSPS",
    help="Number of gsps data to pull",
    type=click.STRING,
)
def app(
    db_url: str,
    datetime_now: Optional[str] = None,
    n_gsps: Optional[int] = N_GSP,
):
    """
    Main App for making metircs

    :param db_url: the database url
    :param datetime_now: the datetime now, for making metris
    :param n_gsps: the number of gsps we should use
    """
    # Get the environment variables to determine which metrics to run
    run_metrics = os.getenv("RUN_METRICS", "true").lower() == "true"
    run_me = os.getenv("RUN_ME", "true").lower() == "true"

    logger.info(f"Running Metrics app ({nowcasting_metrics.__version__})")
    n_gsps = int(n_gsps)

    if datetime_now is None:
        datetime_now = datetime.now(tz=timezone.utc).date()
    else:
        datetime_now = datetime.strptime(datetime_now, "%Y-%m-%d")
        datetime_now = datetime_now.date()

    logger.debug(f"datetime_now is {datetime_now}")

    connection = DatabaseConnection(url=db_url, base=Base_Forecast, echo=False)
    with connection.get_session() as session:
        # check metrics are in the database
        check_metrics_in_database(session=session)

        # get start and end datetime
        start_datetime = datetime_now - timedelta(days=1)
        start_datetime = datetime.combine(start_datetime, datetime.min.time())
        end_datetime = start_datetime + timedelta(days=1)
        datetime_interval = get_datetime_interval(
            start_datetime_utc=start_datetime, end_datetime_utc=end_datetime, session=session
        )
        logger.debug(f"Will be running metrics for {start_datetime} to {end_datetime}")


        try:
            # Check if RUN_METRICS is enabled (default: true). If true, run standard forecast evaluation metrics
            if run_metrics:

                # get data
                all_forecast_values = get_all_forecast_values(session=session, forecast_created_utc=start_datetime)
                gsp_yields_df = get_gsp_yield(session=session, gsp_id=0, start_datetime=start_datetime)

                # run daily MAE
                make_mae(session=session,
                         datetime_interval=datetime_interval,
                         n_gsps=n_gsps,
                         all_forecast_values=all_forecast_values,
                         gsp_yields=gsp_yields_df)

                # run daily RMSE
                # make_rmse(session=session, datetime_interval=datetime_interval, n_gsps=n_gsps)

                # run ramp rate
                make_ramp_rate(session=session,
                               datetime_interval=datetime_interval,
                               all_forecast_values=all_forecast_values,
                               gsp_yields=gsp_yields_df)

                # run probabilistic metrics
                make_probabilistic(session=session,
                                   datetime_interval=datetime_interval,
                                   all_forecast_values=all_forecast_values,
                                   gsp_yields=gsp_yields_df)

            # Check if RUN_ME is enabled (default: true). If true, compute the Mean Error (ME) metric separately
            if run_me:
                # get start and end datetime for 1 week ago
                start_datetime = datetime_now - timedelta(days=7)
                start_datetime = datetime.combine(start_datetime, datetime.min.time())
                end_datetime = start_datetime + timedelta(days=7)
                datetime_interval = get_datetime_interval(
                    start_datetime_utc=start_datetime, end_datetime_utc=end_datetime, session=session
                )
                logger.debug(f"Will be running metrics for {start_datetime} to {end_datetime}")

                all_forecast_values = get_all_forecast_values(session=session)
                gsp_yields_df = get_gsp_yield(session=session, gsp_id=0)

                # getting half hour metrics
                make_me(session=session, datetime_interval=datetime_interval,all_forecast_values=all_forecast_values, gsp_yields=gsp_yields_df)

            # save values to database
            session.commit()

            # Logging that service has finished.
            logger.info("Metrics service has finished processing.")
        except MemoryError:
            # Log if the service stops due to memory issues
            logger.error("Metrics service stopped due to memory issues.")
        except Exception as e:
            raise e

        logger.info("Metrics app service finished")



if __name__ == "__main__":
    app()
