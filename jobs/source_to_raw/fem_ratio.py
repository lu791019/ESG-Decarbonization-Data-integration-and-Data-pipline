from datetime import datetime as dt

from app.logger import logger
from helpers.decarb_date import DecarbDate
from models import engine


def fem_ratio_cal(site, consumtype, start_time, end_time):

    FEM_elect = engine.pd_read_sql(
        f"""SELECT plant AS "plant_code", datadate, power FROM raw.wks_mfg_fem_dailypower where site in ('{site}') and datadate >= '{start_time}' and  datadate<= '{end_time}' and consumetype = '{consumtype}'""")
    plant_map = engine.pd_read_sql(
        f"""SELECT DISTINCT site,plant_name AS "plant",plant_code FROM raw.plant_mapping where site in ('{site}')""")

    df = FEM_elect.merge(plant_map, on='plant_code', how='left').dropna()
    df['power'] = df.groupby(['plant'])['power'].transform('sum')
    df.drop(['datadate'], axis=1, inplace=True)
    df.drop_duplicates(inplace=True)
    df['ratio'] = df['power'].div(df['power'].sum())
    df['period_start'] = str(start_time)
    df.rename(columns={'power': 'amount'}, inplace=True)

    return df


def main():
    start_time = DecarbDate.start_time()
    end_time = DecarbDate.end_time()
    logger.info('fem_ratio start_time: %s, end_time: %s', start_time, end_time)

    logger.info('fem_ratio_cal')
    df = fem_ratio_cal('WKS', '用電量', start_time, end_time)
    df = df[['amount', 'plant', 'ratio', 'period_start']]
    df['category'] = 'plant'
    df['last_update_time'] = dt.strptime(
        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

    plant = df['plant']
    period_start = df['period_start']
    category = df['category']

    if len(plant) == 0 or len(period_start) == 0:

        pass

    elif df.size == 0:

        pass

    else:
        logger.info('delete_query plant: %s, period_start: %s, category: %s',
                    tuple(plant), tuple(period_start), tuple(category))
        delete_query = f"""DELETE FROM raw.fem_ratio WHERE plant IN {tuple(plant)} AND period_start IN {tuple(period_start)} AND category IN {tuple(category)}"""
        engine.execute_sql(delete_query)

        engine.pd_to_sql('fem_ratio', df, schema='raw',
                         if_exists='append', index=False, chunksize=1000)

    return True
