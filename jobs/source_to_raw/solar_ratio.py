from datetime import datetime as dt

from app.logger import logger
from helpers.decarb_date import DecarbDate
from models import engine


def main():
    start_time = DecarbDate.start_time()
    end_time = DecarbDate.end_time()
    logger.info('fem_ratio start_time: %s, end_time: %s', start_time, end_time)

    period_start = start_time
    WZS_solar = engine.pd_read_sql(
        f"""SELECT plant, amount, period_start FROM raw.renewable_energy where period_start = '{period_start}' and category1 ='綠色能源' and category2 ='光伏' and plant in ('WZS-1','WZS-3','WZS-6','WZS-8')""")
    WZS_solar['ratio'] = WZS_solar['amount'].div(WZS_solar['amount'].sum())

    WZS_solar.dropna(inplace=True)
    WZS_solar.drop_duplicates(inplace=True)

    period_start = WZS_solar['period_start'].astype(str)
    plant = WZS_solar['plant']

    WZS_solar['last_update_time'] = dt.strptime(
        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

    if len(period_start) == 0 or len(plant) == 0:

        pass

    elif WZS_solar.size == 0:

        pass

    else:

        delete_query = f"""DELETE FROM raw.solar_ratio WHERE plant IN {tuple(plant)} AND period_start IN {tuple(period_start)}"""

        engine.execute_sql(delete_query)

        table_name = 'solar_ratio'
        engine.pd_to_sql(str(table_name), WZS_solar, schema='raw',
                         if_exists='append', index=False, chunksize=1000)

    return True
