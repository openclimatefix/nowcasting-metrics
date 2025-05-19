from datetime import datetime, timedelta

import logging
import pandas as pd

from nowcasting_datamodel.models.gsp import LocationSQL, GSPYieldSQL
from nowcasting_datamodel.models.models import MLModelSQL

from sqlalchemy import select

logger = logging.getLogger(__name__)


def get_gsp_yield(session, gsp_id: int) -> pd.DataFrame:
    """
    Get all forecast values for the last seven days for a given model name.

    :param session: database session
    :param gsp_id: the gsp id
    :return: gsp_yield_df: dataframe of gsp yields with columns
        datetime_utc and solar_generation_kw
    """
    logger.info(f"Getting gsp yields for model {gsp_id} from the database")
    logger.debug("getting location ids")
    query = session.query(LocationSQL.id)
    query = query.where(LocationSQL.gsp_id == gsp_id)

    locations_ids = query.all()
    locations_ids = [m.id for m in locations_ids]
    logger.debug("got location ids")
    query = select(GSPYieldSQL.datetime_utc, GSPYieldSQL.solar_generation_kw)

    # distinct on datetime_utc
    query = query.distinct(GSPYieldSQL.datetime_utc)

    # filter forecast is
    query = query.filter(GSPYieldSQL.location_id.in_(locations_ids))
    query = query.filter(GSPYieldSQL.datetime_utc >= datetime.now() - timedelta(days=8))
    query = query.filter(GSPYieldSQL.regime == "day-after")

    # order by datetime_utc and created_utc desc
    query = query.order_by(GSPYieldSQL.datetime_utc, GSPYieldSQL.created_utc.desc())

    gsp_yield_df = pd.read_sql_query(
        query, session.bind, index_col="datetime_utc", parse_dates=["datetime_utc"]
    )
    logger.debug(f"got gsp yields, last seven day table, found {len(gsp_yield_df)}.")

    # add tz info to datetime_utc
    gsp_yield_df.index = gsp_yield_df.index.tz_localize("UTC")

    return gsp_yield_df
