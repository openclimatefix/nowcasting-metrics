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

import click
from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models.metric import DatetimeInterval

import nowcasting_metrics
from nowcasting_metrics.metrics.mae import make_mae
from nowcasting_metrics.metrics.metrics import check_metrics_in_database
from nowcasting_metrics.metrics.rmse import make_rmse

logging.basicConfig(
    level=getattr(logging, os.getenv("LOGLEVEL", "DEBUG")),
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


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

    logger.info(f"Running Metrics app ({nowcasting_metrics.__version__})")
    n_gsps = int(n_gsps)

    if datetime_now is None:
        datetime_now = datetime.now(tz=timezone.utc).date()
    else:
        datetime_now = datetime.strptime(datetime_now, "%Y-%M-%d")
        datetime_now = datetime_now.date()

    connection = DatabaseConnection(url=db_url, base=Base_Forecast, echo=False)
    with connection.get_session() as session:

        # check metrics are in the datbase
        check_metrics_in_database(session=session)

        # get start and end datetime
        start_datetime = datetime_now - timedelta(days=1)
        start_datetime = datetime.combine(start_datetime, datetime.min.time())
        end_datetime = start_datetime + timedelta(days=1)
        datetime_interval = DatetimeInterval(
            start_datetime_utc=start_datetime, end_datetime_utc=end_datetime
        )
        logger.debug(f"Will be running metrics for {start_datetime} to {end_datetime}")

        # run daily MAE
        make_mae(session=session, datetime_interval=datetime_interval, n_gsps=n_gsps)

        # run daily RMSE
        make_rmse(session=session, datetime_interval=datetime_interval, n_gsps=n_gsps)

        # save values to database
        session.commit()
