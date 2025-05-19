from datetime import datetime, timedelta

import logging
import pandas as pd
from sqlalchemy.orm.session import Session

from nowcasting_datamodel.models.gsp import LocationSQL
from nowcasting_datamodel.models.forecast import ForecastSQL, ForecastValueSevenDaysSQL
from nowcasting_datamodel.models.models import MLModelSQL

from nowcasting_metrics.metrics.utils import default_national_models

from sqlalchemy import select

logger = logging.getLogger(__name__)


def get_forecast_values(session:Session, model_name: str) -> pd.DataFrame:
    """
    Get all forecast values for the last seven days for a given model name.

    :param session:
    :param model_name:
    :return:
    """
    logger.info(f"Getting forecast values for model {model_name} from the database")
    logger.debug("getting forecast ids")
    query = session.query(ForecastSQL.id)
    query = query.join(MLModelSQL)
    query = query.join(LocationSQL)
    query = query.where(LocationSQL.gsp_id == 0)
    query = query.where(MLModelSQL.name == model_name)

    # fitler on created uct last 2 weeks
    start_date = datetime.now() - timedelta(days=21)
    query = query.where(ForecastSQL.created_utc >= start_date)

    forecasts_ids = query.all()
    forecasts_ids = [m.id for m in forecasts_ids]
    logger.debug("got forecast ids")

    query = select(
        ForecastValueSevenDaysSQL.target_time,
        ForecastValueSevenDaysSQL.expected_power_generation_megawatts,
        ForecastValueSevenDaysSQL.adjust_mw,
        ForecastValueSevenDaysSQL.created_utc,
    )

    # filter forecast is
    query = query.filter(ForecastValueSevenDaysSQL.forecast_id.in_(forecasts_ids))

    # order by target_time and created_utc desc
    query = query.order_by(ForecastValueSevenDaysSQL.target_time, ForecastValueSevenDaysSQL.created_utc.desc())

    forecast_values_df = pd.read_sql_query(
        query, session.bind, index_col="target_time", parse_dates=["target_time", "created_utc"]
    )
    logger.debug(
        f"got forecast values, last seven day table, found {len(forecast_values_df)} forecasts"
    )

    return forecast_values_df


def get_all_forecast_values(session: Session) -> dict:
    """
    Get all forecast values for the last seven days for a given model name.

    :param session: database session
    :return: dictionary of dataframes for each model
    """
    logger.info(f"Getting forecast values from the database")
    logger.debug("getting forecast ids")

    # get all models
    models = default_national_models

    # get all forecast values
    forecast_values = {}
    for model in models:
        forecast_values[model] = get_forecast_values(session, model)

    return forecast_values