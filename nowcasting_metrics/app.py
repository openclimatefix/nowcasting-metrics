"""
This application will run metrics on the nowcasting forecast

1. Get all the metrics we want to use

2. Run each metric and save to database

"""

from datetime import datetime, timezone, timedelta

from nowcasting_metrics.metrics.mae import make_mae
from nowcasting_metrics.metrics.rmse import make_rmse

from nowcasting_datamodel.models.metric import DatetimeInterval


def app():

    # TODO add version

    # Make database connection and session
    # TODO
    session = None

    # get all metrics, and make sure they are in the database

    # run daily metrics

    # get start and end datetime
    yesterday_start_datetime = datetime.now(tz=timezone.utc).date() - timedelta(days=1)
    yesterday_start_datetime = datetime.combine(yesterday_start_datetime, datetime.min.time())
    yesterday_end_datetime = yesterday_start_datetime + timedelta(days=1)
    datetime_interval = DatetimeInterval(
        start_datetime_utc=yesterday_start_datetime, end_datetime_utc=yesterday_end_datetime
    )

    # run daily MAE
    make_mae(
        session=session,
        datetime_interval=datetime_interval,
    )

    # run daily RMSE
    make_rmse(
        session=session,
        datetime_interval=datetime_interval,
    )
