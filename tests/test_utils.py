import datetime
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Import the functions and classes we need from your code.
from nowcasting_metrics.utils import (
    make_forecast_sub_query,
    save_metric_value_to_database
)
from nowcasting_datamodel.models import MetricSQL
from nowcasting_datamodel.models.metric import DatetimeInterval

#
# ──────────────────────────────────────────────────────────────────────────────
#  Existing Test for `save_metric_value_to_database`
# ──────────────────────────────────────────────────────────────────────────────
#
def test_save_metric_value_to_database_location_none(db_session, datetime_interval):
    """
    Test saving a metric value=None to the database with location=None
    using `save_metric_value_to_database`.
    """
    metric = MetricSQL(name="test_model", description="test model")

    save_metric_value_to_database(
        session=db_session,
        value=None,
        number_of_data_points=0,
        metric=metric,
        datetime_interval=datetime_interval,
    )


#
# ──────────────────────────────────────────────────────────────────────────────
#  New Tests for `make_forecast_sub_query`
# ──────────────────────────────────────────────────────────────────────────────
#
def test_make_forecast_sub_query_neso_solar_forecast(db_session):
    """
    Test that using model_name="neso-solar-forecast" leads to a subquery
    referencing the 14-day forecast model.
    """
    # Create a dummy datetime interval
    dummy_interval = DatetimeInterval(
        start_datetime_utc=datetime.datetime(2022, 1, 1, 0, 0, 0),
        end_datetime_utc=datetime.datetime(2022, 1, 2, 0, 0, 0)
    )

    subquery = make_forecast_sub_query(
        datetime_interval=dummy_interval,
        forecast_horizon_minutes=60,
        gsp_id=1,
        session=db_session,
        model_name="neso-solar-forecast",
    )

    # Convert the subquery to a string so we can check if it references the 14-day model
    query_str = str(subquery)
    assert "ForecastValueFourteenDaysSQL" in query_str, (
        "Expected the subquery to reference the 14-day model for neso-solar-forecast"
    )


def test_make_forecast_sub_query_cnn(db_session):
    """
    Test that using model_name="cnn" leads to a subquery
    referencing the 7-day forecast model (the default).
    """
    # Create a dummy datetime interval
    dummy_interval = DatetimeInterval(
        start_datetime_utc=datetime.datetime(2022, 1, 1, 0, 0, 0),
        end_datetime_utc=datetime.datetime(2022, 1, 2, 0, 0, 0)
    )

    subquery = make_forecast_sub_query(
        datetime_interval=dummy_interval,
        forecast_horizon_minutes=60,
        gsp_id=1,
        session=db_session,
        model_name="cnn",
    )

    # Convert the subquery to a string so we can check if it references the 7-day model
    query_str = str(subquery)
    assert "ForecastValueSevenDaysSQL" in query_str, (
        "Expected the subquery to reference the 7-day model for cnn"
    )
