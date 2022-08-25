import os

import pytest
from datetime import datetime
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_Forecast, Base_PV

from nowcasting_datamodel.read.read import get_location

from nowcasting_datamodel.models.gsp import GSPYield
from nowcasting_datamodel.models.models import ForecastValueLatestSQL
from nowcasting_datamodel.models.metric import DatetimeInterval


@pytest.fixture
def db_connection():

    url = os.getenv("DB_URL", "sqlite:///test.db")
    os.environ["DB_URL"] = url

    connection = DatabaseConnection(url=url)
    Base_Forecast.metadata.create_all(connection.engine)
    Base_PV.metadata.create_all(connection.engine)

    yield connection

    Base_Forecast.metadata.drop_all(connection.engine)
    Base_PV.metadata.drop_all(connection.engine)


@pytest.fixture(scope="function", autouse=True)
def db_session(db_connection):
    """Creates a new database session for a test."""

    with db_connection.get_session() as s:
        s.begin()
        yield s
        s.rollback()


@pytest.fixture
def forecast_values_latest(db_session):

    dt1 = datetime(2022, 1, 1, 0, 30)
    dt2 = datetime(2022, 1, 1, 1)

    forecast_values_latest_1 = ForecastValueLatestSQL(
        target_time=dt1, expected_power_generation_megawatts=1, gsp_id=1
    )
    forecast_values_latest_2 = ForecastValueLatestSQL(
        target_time=dt2, expected_power_generation_megawatts=4, gsp_id=1
    )

    db_session.add_all([forecast_values_latest_1, forecast_values_latest_2])


@pytest.fixture
def gsp_yields(db_session):
    dt1 = datetime(2022, 1, 1, 0, 30)
    dt2 = datetime(2022, 1, 1, 1)

    location = get_location(session=db_session, gsp_id=1)

    gsp_yield_1 = GSPYield(datetime_utc=dt1, solar_generation_kw=1000, regime="day-after").to_orm()
    gsp_yield_2 = GSPYield(datetime_utc=dt2, solar_generation_kw=2000, regime="day-after").to_orm()
    gsp_yield_1.location = location
    gsp_yield_2.location = location

    db_session.add_all([gsp_yield_1, gsp_yield_2])


@pytest.fixture
def datetime_interval():

    return DatetimeInterval(
        start_datetime_utc=datetime(2022, 1, 1), end_datetime_utc=datetime(2022, 1, 2)
    )
