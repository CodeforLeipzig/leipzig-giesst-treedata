import logging
import os

import geopandas as gpd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_DIR = os.path.abspath(os.curdir)


def get_connection_dict():
    for env_var in ["PG_DB", "PG_PORT", "PG_USER", "PG_PASS", "PG_DB"]:
        if env_var not in os.environ:
            msg = f"Environmental variable {env_var} does not exist but is required"
            logger.error(msg)
            raise Exception(msg)
    pg_server = os.getenv("PG_SERVER")
    pg_port = os.getenv("PG_PORT")
    pg_username = os.getenv("PG_USER")
    pg_password = os.getenv("PG_PASS")
    pg_database = os.getenv("PG_DB")
    return {
        pg_server: pg_server,
        pg_port: pg_port,
        pg_username: pg_username,
        pg_password: pg_password,
        pg_database: pg_database
    }


def get_db_engine():
    pg_server, pg_port, pg_username, pg_password, pg_database = get_connection_dict()

    conn_string = f"postgresql://{pg_username}:{pg_password}@{pg_server}:{pg_port}/{pg_database}"

    return create_engine(conn_string, connect_args={"options": "-c statement_timeout=300000"})


def add_to_db(engine, result, table_name):
    result['geometry'] = gpd.points_from_xy(result.lng, result.lat)
    result = result.rename(columns={'geometry': 'geom'}).set_geometry('geom')
    result.to_postgis(table_name, engine, if_exists='replace', index=False)
    logger.info(f'Imported data to table {table_name}')
