""" Script to see how good the adjuster is doing

This only works for the last 7 days
"""


import json
from datetime import datetime, timedelta

import boto3
import pandas as pd
import plotly.graph_objects as go
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.read.read_metric import get_datetime_interval
from plotly.subplots import make_subplots

from nowcasting_metrics.metrics.mae import make_mae_one_gsp_with_forecast_horizon
from nowcasting_metrics.metrics.mae import make_pvlive_mae

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
forecast_horizons = [0, 60, 240, 480]

with connection.get_session() as session:

    start = datetime(2023, 3, 10)
    results = []

    for day in range(0, 7):

        start_datetime_utc = start + timedelta(days=day)
        end_datetime_utc = start_datetime_utc + timedelta(days=1)

        datetime_interval = get_datetime_interval(
            session=session,
            start_datetime_utc=start_datetime_utc,
            end_datetime_utc=end_datetime_utc,
        )

        print(start_datetime_utc)

        mae, _ = make_pvlive_mae(session=session, datetime_interval=datetime_interval, gsp_id=0)
        print(f"PV LIVE {mae=}")

        results.append(
            {
                "mae": mae,
                "forecast_horizon": -1,
                "start_datetime_utc": start_datetime_utc,
                "use_adjuster": False,
            }
        )

        for forecast_horizon in forecast_horizons:
            print("")
            for use_adjuster in [True, False]:

                print(f"{forecast_horizon=} {day=} {use_adjuster=}")

                mae, _ = make_mae_one_gsp_with_forecast_horizon(
                    session=session,
                    gsp_id=0,
                    forecast_horizon_minutes=forecast_horizon,
                    datetime_interval=datetime_interval,
                    use_adjuster=use_adjuster,
                )
                print(mae)

                results.append(
                    {
                        "mae": mae,
                        "forecast_horizon": forecast_horizon,
                        "start_datetime_utc": start_datetime_utc,
                        "use_adjuster": use_adjuster,
                    }
                )


result_df = pd.DataFrame(results)

fig = make_subplots(
    rows=2,
    cols=2,
    subplot_titles=[str(f) for f in forecast_horizons],
)

pvlive = result_df[result_df["forecast_horizon"] == -1]

for i in range(len(forecast_horizons)):
    forecast_horizon = forecast_horizons[i]

    results_one_forecast_horizon = result_df[result_df["forecast_horizon"] == forecast_horizon]
    non_adjust = results_one_forecast_horizon[results_one_forecast_horizon["use_adjuster"] == False]
    adjust = results_one_forecast_horizon[results_one_forecast_horizon["use_adjuster"] == True]

    row = i % 2 + 1
    col = i // 2 + 1

    showlegend = False
    if i == 1:
        showlegend = True

    fig.add_trace(
        go.Scatter(
            x=adjust["start_datetime_utc"],
            y=adjust["mae"],
            name="With Adjuster",
            line=dict(color="red", dash="solid"),
            showlegend=showlegend,
        ),
        row=row,
        col=col,
    )

    fig.add_trace(
        go.Scatter(
            x=non_adjust["start_datetime_utc"],
            y=non_adjust["mae"],
            name="No Adjuster",
            line=dict(color="blue", dash="solid"),
            showlegend=showlegend,
        ),
        row=row,
        col=col,
    )

    fig.add_trace(
        go.Scatter(
            x=pvlive["start_datetime_utc"],
            y=pvlive["mae"],
            name="PVLive",
            line=dict(color="black", dash="dash"),
            showlegend=showlegend,
        ),
        row=row,
        col=col,
    )

fig.update_layout(
    title="MAE with adjuster (and not)",
    yaxis_title="MAE [MW]",
)

fig.show(renederer="browser")
