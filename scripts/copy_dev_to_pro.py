import os
from datetime import datetime

url_dev = os.getenv("DB_URL_DEV")
url_pro = os.getenv("DB_URL_PRO")

from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models.metric import MetricValueSQL, DatetimeIntervalSQL, MetricValue, MetricSQL
from nowcasting_datamodel.models.models import MLModelSQL
from nowcasting_datamodel.read.read import get_location
from nowcasting_datamodel.read.read_metric import get_metric
from nowcasting_datamodel.read.read_metric import get_datetime_interval
from nowcasting_datamodel.read.read import get_model


connection = DatabaseConnection(url=url_dev, base=Base_Forecast, echo=False)


end_date = datetime(2024, 3, 18)

with connection.get_session() as session:

    query = session.query(MetricValueSQL)
    query = query.distinct(MetricValueSQL.forecast_horizon_minutes,MetricValueSQL.time_of_day)

    query = query.join(DatetimeIntervalSQL)
    query = query.join(MLModelSQL)
    query = query.join(MetricSQL)

    query = query.where(DatetimeIntervalSQL.end_datetime_utc >= end_date)

    query = query.where(MetricValueSQL.forecast_horizon_minutes <= 8*60)

    query = query.where(MLModelSQL.name == 'pvnet_v2')
    query = query.where(MetricSQL.name == 'Half Hourly ME')

    query = query.order_by(MetricValueSQL.forecast_horizon_minutes,MetricValueSQL.time_of_day, MetricValueSQL.created_utc.desc())

    metric_values = query.all()

    model_name = metric_values[0].model.name
    metric_values = [MetricValue.from_orm(metric_value) for metric_value in metric_values]

#
# connection = DatabaseConnection(url=url_pro, base=Base_Forecast, echo=False)
# with connection.get_session() as session:
#     metric_values_new = [metric_value.to_orm() for metric_value in metric_values]
#
#     model = get_model(session=session, name=model_name)
#     datetime = get_datetime_interval(session=session,
#                                      start_datetime_utc=metric_values[0].datetime_interval.start_datetime_utc,
#                                      end_datetime_utc=metric_values_new[0].datetime_interval.end_datetime_utc)
#     location = get_location(session=session, gsp_id=metric_values_new[0].location.gsp_id)
#     metric = get_metric(session=session, name=metric_values_new[0].metric.name)
#
#     for metric_value in metric_values_new:
#         metric_value.model = model
#         metric_value.datetime_interval = datetime
#         metric_value.location = location
#         metric_value.metric = metric
#
#
connection = DatabaseConnection(url=url_pro, base=Base_Forecast, echo=False)
with connection.get_session() as session:
    metric_values_new = [metric_value.to_orm() for metric_value in metric_values]

    model = get_model(session=session, name=model_name)
    datetime = get_datetime_interval(session=session,
                                     start_datetime_utc=metric_values[0].datetime_interval.start_datetime_utc,
                                     end_datetime_utc=metric_values_new[0].datetime_interval.end_datetime_utc)
    location = get_location(session=session, gsp_id=metric_values_new[0].location.gsp_id)
    metric = get_metric(session=session, name=metric_values_new[0].metric.name)

    for metric_value in metric_values_new:
        metric_value.model = model
        metric_value.datetime_interval = datetime
        metric_value.location = location
        metric_value.metric = metric

    print(metric_values_new[0].__dict__)
    session.add_all(metric_values_new)
    session.commit()

#
#
#
#
#
#
