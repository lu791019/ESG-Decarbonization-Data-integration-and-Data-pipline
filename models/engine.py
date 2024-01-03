import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.logger import logger
from config import Config


def get_connect_string():
    connection_string = sa.engine.URL.create(
        drivername="postgresql+psycopg2",
        username=Config.ECO_SSOT_RDS_USERNAME,
        password=Config.ECO_SSOT_RDS_PASSWORD,
        host=Config.ECO_SSOT_RDS_HOST,
        port=int(Config.ECO_SSOT_RDS_PORT) if Config.ECO_SSOT_RDS_PORT else 5432,
        database=Config.ECO_SSOT_RDS_DATABASE,
    )

    return f"{connection_string}?application_name=DECARB-ETL ({Config.FLASK_ENV})"


engine = create_engine(get_connect_string())

# orm used
SessionMaker = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Session = Session(autocommit=False, autoflush=False, bind=engine)


def execute_sql(sql, params=None):
    with engine.begin() as conn:
        return conn.execute(text(sql), params)


def pd_read_sql(sql, params=None):
    logger.info('sql: %s, params: %s', sql, params)
    return pd.read_sql(sql, engine, params=params)


def pd_to_sql(sql, data_frame, **params):
    logger.info('sql: %s, params: %s', sql, params)
    params.setdefault('schema', None)
    params.setdefault('if_exists', "fail")
    params.setdefault('index', True)
    params.setdefault('index_label', None)
    params.setdefault('chunksize', None)
    params.setdefault('dtype', None)
    params.setdefault('method', None)

    return data_frame.to_sql(sql, engine,
                             schema=params['schema'],
                             if_exists=params['if_exists'],
                             index=params['index'],
                             index_label=params['index_label'],
                             chunksize=params['chunksize'],
                             dtype=params['dtype'],
                             method=params['method'],
                             )
