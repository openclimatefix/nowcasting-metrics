from nowcasting_metrics.database.forecast import get_forecast_values, get_all_forecast_values
from freezegun import freeze_time

@freeze_time("2022-01-01 00:00:00")
def test_get_forecast_values(db_session, forecast_values):
    """
    Test that the get_forecast_values function returns the correct forecast values
    """
    # need this for the test to work
    db_session.commit()

    forecast_values = get_forecast_values(session=db_session, model_name='pvnet_v2')

    # Check that the forecast values are correct
    assert len(forecast_values) == 16

