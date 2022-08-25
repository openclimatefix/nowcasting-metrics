from nowcasting_metrics.metrics.rmse import make_rmse_one_gsp


def test_make_mae(db_session, gsp_yields, forecast_values_latest, datetime_interval):

    value, n = make_rmse_one_gsp(session=db_session, datetime_interval=datetime_interval, gsp_id=1)

    assert value == 2.25  # ((1-1)*0.5 + (4-1)**2*0.5)^0.5 = 4.5^0.5
    assert n == 2
