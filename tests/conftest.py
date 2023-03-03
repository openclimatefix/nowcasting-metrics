from datetime import datetime, timedelta

import pytest
from nowcasting_datamodel.models import ForecastSQL, ForecastValueLatestSQL, ForecastValueSQL
from nowcasting_datamodel.models.base import Base_Forecast, Base_PV
from nowcasting_datamodel.models.forecast import get_partitions
from nowcasting_datamodel.models.gsp import GSPYield
from nowcasting_datamodel.models.metric import DatetimeInterval
from nowcasting_datamodel.read.read import get_location
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def engine():
    """Database engine fixture."""
    with PostgresContainer("postgres:14.5") as postgres:
        # TODO need to setup postgres database with docker
        url = postgres.get_connection_url()
        engine = create_engine(url)
        Base_Forecast.metadata.create_all(engine)
        Base_PV.metadata.create_all(engine)

        partitions = get_partitions(2019, 1, 2022, 7)

        # make partitions
        for partition in partitions:
            if not engine.dialect.has_table(
                connection=engine.connect(), table_name=partition.__table__.name
            ):
                partition.__table__.create(bind=engine)

        yield engine


@pytest.fixture()
def db_session(engine):
    """Return a sqlalchemy session, which tears down everything properly post-test."""
    connection = engine.connect()
    # begin the nested transaction
    transaction = connection.begin()
    # use the connection with the already started transaction

    with Session(bind=connection) as session:
        yield session

        session.close()
        # roll back the broader transaction
        transaction.rollback()
        # put back the connection to the connection pool
        connection.close()
        session.flush()

    engine.dispose()


@pytest.fixture
def forecast_values_latest(db_session):
    dt1 = datetime(2022, 1, 1, 0, 30)
    dt2 = datetime(2022, 1, 1, 1)

    for gsp_id in range(0, 6):
        forecast_values_latest_1 = ForecastValueLatestSQL(
            target_time=dt1, expected_power_generation_megawatts=1, gsp_id=gsp_id
        )
        forecast_values_latest_2 = ForecastValueLatestSQL(
            target_time=dt2, expected_power_generation_megawatts=4, gsp_id=gsp_id
        )

        db_session.add_all([forecast_values_latest_1, forecast_values_latest_2])


@pytest.fixture
def forecast_values(db_session):
    dt1 = datetime(2022, 1, 1, 0, 30)
    dt2 = datetime(2022, 1, 1, 1)

    for gsp_id in range(0, 6):
        location = get_location(gsp_id=gsp_id, session=db_session)

        for forecast_horizon_minutes in range(0, 240, 30):
            created_utc_1 = dt1 - timedelta(minutes=forecast_horizon_minutes + 15)
            created_utc_2 = dt2 - timedelta(minutes=forecast_horizon_minutes + 15)

            forecast_values_1 = ForecastValueSQL(
                target_time=dt1,
                expected_power_generation_megawatts=1 + forecast_horizon_minutes,
                created_utc=created_utc_1,
            )
            forecast_values_2 = ForecastValueSQL(
                target_time=dt2,
                expected_power_generation_megawatts=4 + forecast_horizon_minutes,
                created_utc=created_utc_2,
            )

            forecast = ForecastSQL(
                location=location,
                forecast_values=[forecast_values_1, forecast_values_2],
            )

            db_session.add(forecast)


@pytest.fixture
def gsp_yields(db_session):
    dt1 = datetime(2022, 1, 1, 0, 30)
    dt2 = datetime(2022, 1, 1, 1)

    for gsp_id in range(0, 6):
        location = get_location(session=db_session, gsp_id=gsp_id)

        gsp_yield_1 = GSPYield(
            datetime_utc=dt1, solar_generation_kw=1000, regime="day-after"
        ).to_orm()
        gsp_yield_2 = GSPYield(
            datetime_utc=dt2, solar_generation_kw=1000, regime="day-after"
        ).to_orm()
        gsp_yield_1.location = location
        gsp_yield_2.location = location

        db_session.add_all([gsp_yield_1, gsp_yield_2])


@pytest.fixture
def gsp_yields_inday(db_session):
    dt1 = datetime(2022, 1, 1, 0, 30)
    dt2 = datetime(2022, 1, 1, 1)

    for gsp_id in range(0, 6):
        location = get_location(session=db_session, gsp_id=gsp_id)

        gsp_yield_1 = GSPYield(datetime_utc=dt1, solar_generation_kw=2000, regime="in-day").to_orm()
        gsp_yield_2 = GSPYield(datetime_utc=dt2, solar_generation_kw=2000, regime="in-day").to_orm()
        gsp_yield_1.location = location
        gsp_yield_2.location = location

        db_session.add_all([gsp_yield_1, gsp_yield_2])


@pytest.fixture
def datetime_interval():
    return DatetimeInterval(
        start_datetime_utc=datetime(2022, 1, 1), end_datetime_utc=datetime(2022, 1, 2)
    )
