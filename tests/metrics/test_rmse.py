from nowcasting_metrics.metrics.rmse import make_rmse_one_gsp, make_rmse


def test_make_rmse(db_session, gsp_yields, forecast_values_latest, datetime_interval):

    value, n = make_rmse_one_gsp(session=db_session, datetime_interval=datetime_interval, gsp_id=1)

    assert value == 4.5**0.5  # ((1-1)*0.5 + (4-1)**2*0.5)^0.5 = 4.5^0.5
    assert n == 2


def test_make_rmse_on_gsp(db_session, gsp_yields, forecast_values_latest, datetime_interval):

    make_rmse(session=db_session, datetime_interval=datetime_interval, n_gsps=5)
