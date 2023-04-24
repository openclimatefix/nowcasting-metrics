from datetime import datetime, timedelta

start_date = datetime(2022, 9, 1)
end_date = datetime(2022, 9, 1)

import json

import boto3
import pandas as pd
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.read.read_metric import get_datetime_interval
from nowcasting_datamodel.models import (
    ForecastValueSevenDaysSQL,
    ForecastValueSQL,
    GSPYieldSQL,
    MLModelSQL,
    ForecastSQL,
)

from functools import lru_cache
from nowcasting_metrics.metrics.mae import (
    make_mae_query,
    make_gsp_sub_query,
    make_forecast_sub_query,
)

import plotly.graph_objects as go

# laod database secret from AWS secrets
client = boto3.client("secretsmanager")
response = client.get_secret_value(
    SecretId="development/rds/forecast/",
)
secret = json.loads(response["SecretString"])
""" We have used a ssh tunnel to 'localhost' """
db_url = f'postgresql://{secret["username"]}:{secret["password"]}@localhost:5433/{secret["dbname"]}'

# make database connection
connection = DatabaseConnection(url=db_url, base=Base_Forecast, echo=False)


start_date = datetime(2023, 4, 5)
end_date = datetime(2023, 4, 14)
gsp_id = 0
forecast_horizon_minutes = [60, 120, 240, 480, 12*60, 18*60, 24*60, 30*60]
forecast_horizon_minutes = [0, 60, 120, 240, 480]
colours = ["red", "blue", "green", "orange", "purple"]
models = ["National_xg", "cnn"]


@lru_cache(maxsize=None)
def run_metric(session, forecast_horizon_minutes, gsp_id, start_date, model):

    print(f"{forecast_horizon_minutes=} {start_date=} {model=}")

    end_date = start_date + timedelta(days=1)

    datetime_interval = get_datetime_interval(
        session=session, start_datetime_utc=start_date, end_datetime_utc=end_date
    )

    f_model = ForecastValueSQL

    sub_query_gsp = make_gsp_sub_query(datetime_interval, gsp_id, session)
    sub_query_forecast = make_forecast_sub_query(
        datetime_interval, forecast_horizon_minutes, gsp_id, session, model=f_model, model_name=model
    )

    # make full query
    query = make_mae_query(session, model=f_model, use_adjuster=True)

    query = query.filter(f_model.uuid.in_(sub_query_forecast))
    query = query.filter(GSPYieldSQL.id.in_(sub_query_gsp))
    query = query.filter(GSPYieldSQL.datetime_utc == f_model.target_time)
    query = query.join(ForecastSQL)
    query = query.join(MLModelSQL)
    query = query.filter(MLModelSQL.name == model)
    results = query.all()

    number_of_data_points = results[0][1]
    value = results[0][0]

    print(value, number_of_data_points)

    return value


def run_all():

    results_df = get_mae()

    traces = []
    # for model in ["cnn", "National_xg"]:
    for model in models:
        for i in range(len(forecast_horizon_minutes)):

            forecast_horizon_minute = forecast_horizon_minutes[i]
            forecast_horizon_hour = int(forecast_horizon_minute / 60)

            if forecast_horizon_hour > 8 and model == 'cnn':
                continue

            results_df_one = results_df[
                results_df["forecast_horizon_minutes"] == forecast_horizon_minute
            ]
            results_df_one = results_df_one[results_df_one["model"] == model]

            if model == 'cnn':
                mode='lines'
                line = dict(color=colours[i], dash='dash')
            else:
                mode='lines+markers'
                line = dict(color=colours[i % len(colours)])
            trace = go.Scatter(
                x=results_df_one.start_date,
                y=results_df_one.value,
                name=f"{model}_{forecast_horizon_hour}H",
                line=line,
                mode=mode,
            )
            traces.append(trace)

    fig = go.Figure()
    fig.add_traces(traces)
    fig.show()


def get_mae():
    results = []
    with connection.get_session() as session:

        # for model in ["cnn", "National_xg"]:
        for model in models:
            for forecast_horizon_minute in forecast_horizon_minutes:
                for days in range(0, 10):

                    date = start_date + timedelta(days=days)
                    print(f'{date=} {model=} {forecast_horizon_minute=}')

                    value = run_metric(
                        session=session,
                        gsp_id=0,
                        forecast_horizon_minutes=forecast_horizon_minute,
                        start_date=date,
                        model=model,
                    )
                    results.append(
                        {
                            "model": model,
                            "value": value,
                            "forecast_horizon_minutes": forecast_horizon_minute,
                            "start_date": date,
                        }
                    )
    results_df = pd.DataFrame(results)


    print(results_df)

    return results_df


if __name__ == "__main__":
    run_all()
