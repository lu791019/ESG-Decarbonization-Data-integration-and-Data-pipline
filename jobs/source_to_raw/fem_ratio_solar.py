from datetime import datetime as dt

from app.logger import logger
from helpers.decarb_date import DecarbDate
from models import engine


def main():
    start_time = DecarbDate.start_time()
    end_time = DecarbDate.end_time()
    logger.info('fem_ratio start_time: %s, end_time: %s', start_time, end_time)

    site = 'WKS'
    consumtype = '用電量'

    FEM_elect = engine.pd_read_sql(
        f"""SELECT plant AS "plant_code", datadate, power FROM raw.wks_mfg_fem_dailypower where site in ('{site}') and datadate >= '{start_time}' and  datadate<= '{end_time}' and consumetype = '{consumtype}'""")
    plant_map = engine.pd_read_sql(
        f"""SELECT DISTINCT site,plant_name AS "plant",plant_code FROM raw.plant_mapping where site in ('{site}','XTRKS')""")
    df = FEM_elect.merge(plant_map, on='plant_code', how='left').dropna()
    df['power'] = df.groupby(['plant'])['power'].transform('sum')
    df.drop(['datadate'], axis=1, inplace=True)
    df.drop_duplicates(inplace=True)
    df['ratio'] = df['power'].div(df['power'].sum())
    df['period_start'] = start_time

    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)

    period_start = df['period_start']
    plant = df['plant']

    df['last_update_time'] = dt.strptime(
        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

    if len(period_start) == 0 or len(plant) == 0:

        pass

    elif df.size == 0:

        pass

    else:

        delete_query = f"""DELETE FROM raw.fem_ratio_solar WHERE plant IN {tuple(plant)} AND period_start IN {tuple(period_start)}"""
        engine.execute_sql(delete_query)

        table_name = 'fem_ratio_solar'
        engine.pd_to_sql(str(table_name), df, schema='raw',
                         if_exists='append', index=False, chunksize=1000)

    return True
