from nowcasting_metrics.database.gsp_yield import get_gsp_yield
from freezegun import freeze_time

@freeze_time("2022-01-01 00:00:00")
def test_get_gsp_yield(db_session, gsp_yields):
    """
    Test that the get_forecast_values function returns the correct forecast values
    """
    db_session.commit()
    gsp_yields_df = get_gsp_yield(session=db_session, gsp_id=0)
    print(gsp_yields_df)

    assert len(gsp_yields_df) == 2

