import os

import pytest
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_Forecast, Base_PV


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
