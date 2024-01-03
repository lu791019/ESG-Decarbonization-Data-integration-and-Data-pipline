import calendar
import os
from datetime import date
from datetime import datetime as dt
from datetime import timedelta

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import *

from app.logger import logger
from models import engine
from services.mail_service import MailService


def get_stage():
    return os.environ['FLASK_ENV'] if 'FLASK_ENV' in os.environ else 'development'


def cal_renew_total(renew_target, df_elect, df_solar, df_grelect, df_grenergy):

    df_renew_target = pd.merge(df_elect, renew_target, on=['year'], how='left')
    df_renew_target['target_renew'] = df_renew_target['total_elect'] * \
        df_renew_target['target_rate']
    df_renew_target = df_renew_target[['site', 'target_renew']]

    df_grey_elect = pd.merge(pd.merge(df_elect, df_solar, on=[
                             'site'], how='left'),  df_grelect, on=['site'], how='left')

    df_grey_elect = df_grey_elect.fillna(0)

    df_grey_elect['grey_elect'] = df_grey_elect['total_elect'] - \
        df_grey_elect['solar'] - df_grey_elect['green_elect']

    df_green_demend = pd.merge(pd.merge(pd.merge(df_renew_target, df_solar, on=[
                               'site'], how='left'),  df_grelect, on=['site'], how='left'),  df_grenergy, on=['site'], how='left')

    df_green_demend = df_green_demend.fillna(0)

    df_green_demend['green_energy_request'] = df_green_demend['target_renew'] - \
        df_green_demend['solar'] - df_green_demend['green_elect'] - \
        df_green_demend['green_energy']

    df_green_demend['green_energy_request'] = df_green_demend['green_energy_request'].apply(
        lambda x: 0 if x < 0 else x)

    df_grey_elect = df_grey_elect[[
        'site', 'year', 'total_elect', 'grey_elect']]

    df_renew_total = pd.merge(
        df_grey_elect, df_green_demend, on=['site'], how='left')

    df_renew_total.drop_duplicates(inplace=True)

    return df_renew_total


def green_energy_overview(run_type):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    stage = get_stage()

    try:
        for i in range(1, dt.now().month+1, 3):

            if i in (1, 2, 3):

                year = dt.now().year-1
                period_start = date(dt.now().year-1, 1,
                                    1).strftime("%Y-%m-%d")
                period_end = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")

                quarter = 'Q4'
                quarter_num = 4

            elif i in (4, 5, 6):

                year = dt.now().year
                period_start = date(dt.now().year, 1, 1).strftime("%Y-%m-%d")
                period_end = date(dt.now().year, 3, 1).strftime("%Y-%m-%d")

                quarter = 'Q1'
                quarter_num = 1

            elif i in (7, 8, 9):

                year = dt.now().year
                period_start = date(dt.now().year, 1, 1).strftime("%Y-%m-%d")
                period_end = date(dt.now().year, 6, 1).strftime("%Y-%m-%d")

                quarter = 'Q2'
                quarter_num = 2

            elif i in (10, 11, 12):

                year = dt.now().year
                period_start = date(dt.now().year, 1, 1).strftime("%Y-%m-%d")
                period_end = date(dt.now().year, 9, 1).strftime("%Y-%m-%d")

                quarter = 'Q3'
                quarter_num = 3

            if run_type == 'add_customer_data':

                if year <=2022:

                    pass

                else:

                    df_elect_add = pd.read_sql(
                        f"""SELECT "year", site, total_elect FROM app.green_energy_amount where quarter = {quarter_num} and year = {year} and customer is not null and customer !='ALL' and customer !=''""", db)

                    df_grelect_add = pd.read_sql(
                        f"""SELECT site, green_elect FROM app.green_energy_amount where quarter = {quarter_num} and year = {year} and customer is not null and customer !='ALL' and customer !=''""", db)

                    df_solar_add = pd.read_sql(
                        f"""SELECT site, solar FROM app.green_energy_amount where quarter = {quarter_num} and year = {year} and customer is not null and customer !='ALL' and customer !=''""", db)

                    df_grenergy_add = pd.read_sql(
                        f"""SELECT site, green_energy FROM app.green_energy_amount where quarter = {quarter_num} and year = {year} and customer is not null and customer !='ALL' and customer !=''""", db)

                    df_grenergy_add['green_energy'] = 0

                    renew_target = pd.read_sql(
                        f"""SELECT "year", sum(amount)/100 as "target_rate" FROM staging.renewable_setting where year = {year} and category in ('solar','PPA','REC') group by "year" """, db)

                    df_renew_total_add = cal_renew_total(
                        renew_target, df_elect_add, df_solar_add, df_grelect_add, df_grenergy_add)

                    df_add_area = pd.read_sql(
                        f"""SELECT "year", quarter, area, site, customer FROM app.green_energy_amount  where quarter = {quarter_num} and year = {year} and customer is not null and customer !='ALL' and customer !=''""", db)

                    renew_add_1 = pd.merge(df_renew_total_add, df_add_area, on=[
                                        'site', 'year'], how='left')

                    unit_price_add = pd.read_sql(
                        f"""SELECT "year", site, customer, unit_price, amount as "actual_amount" FROM app.green_purchase where customer !='-' and customer IS NOT NULL and year ={year} and quarter = '{quarter}'""", db)

                    renew_add = pd.merge(renew_add_1, unit_price_add, on=[
                                        'site', 'year', 'customer'], how='left')

                    renew_add['unit_price'] = renew_add['unit_price'].fillna(0)

                    renew_add['predict_price'] = renew_add['unit_price'] * \
                        renew_add['green_energy_request']

                    renew_add['ratio'] = renew_add['green_energy'] / \
                        renew_add['total_elect'] * 100

                    df_remark = pd.read_sql(f"""SELECT "year", quarter, area, site, customer, remark FROM app.green_energy_amount where year = {year} and quarter = {quarter_num}""",db)

                    renew_add = pd.merge(renew_add, df_remark, on=['year','quarter','site','area','customer'], how='left')

                    year_ = renew_add['year']
                    quarter_ = renew_add['quarter']
                    area_ = renew_add['area']
                    customer_ = renew_add['customer']

                    if len(year_) == 0 or len(customer_) == 0:

                        pass

                    elif renew_add.size == 0:

                        pass

                    elif renew_add.shape[0] == 1:

                        delete_query = f"""DELETE FROM app.green_energy_amount WHERE year  = {year_[0]} AND quarter = {quarter_[0]} AND area = '{area_[0]}' AND customer = '{customer_[0]}'"""

                        conn = db.connect()
                        conn.execute(delete_query)

                        renew_add.to_sql('green_energy_amount', db, index=False,
                                        if_exists='append', schema='app', chunksize=10000)
                        conn.close()

                    else:

                        delete_query = f"""DELETE FROM app.green_energy_amount WHERE year IN {tuple(year_)} AND quarter IN {tuple(quarter_)} AND area IN {tuple(area_)} AND customer IN {tuple(customer_)}"""

                        conn = db.connect()
                        conn.execute(delete_query)

                        renew_add.to_sql('green_energy_amount', db, index=False,
                                        if_exists='append', schema='app', chunksize=10000)
                        conn.close()

            if run_type == 'summarize_all_data':

                # 與DS確認過資料皆以累計觀看 故取ytm_amount ,時間點為Q1:3月份, Q2: 6月份, Q3: 月份 , Q4:12月份 (period_end = 3,6,9,12)
                if year <=2022:

                    pass

                else:
                    renew_target = pd.read_sql(
                        f"""SELECT "year", sum(amount)/100 as "target_rate" FROM staging.renewable_setting where year = {year} and category in ('solar','PPA','REC') group by "year" """, db)

                    df_elect_src = pd.read_sql(
                        f"""SELECT  site, sum(ytm_amount) as "total_elect" FROM staging.electricity_decarb where period_start = '{period_end}' and bo = 'ALL' and site !='ALL' group by site """, db)

                    df_solar_src = pd.read_sql(
                        f"""SELECT site, sum(ytm_amount) as "solar"  FROM staging.renewable_energy_decarb where category = 'solar_energy' and period_start = '{period_end}' and bo = 'ALL' and site !='ALL'  group by site""", db)

                    df_grelect_src = pd.read_sql(
                        f"""SELECT site, sum(ytm_amount) as "green_elect"  FROM staging.renewable_energy_decarb where category = 'green_electricity' and period_start = '{period_end}' and bo = 'ALL' and site !='ALL'  group by site""", db)

                    df_grenergy_src = pd.read_sql(
                        f"""SELECT site, sum(ytm_amount) as "green_energy"  FROM staging.renewable_energy_decarb where category = 'green_energy' and period_start = '{period_end}' and bo = 'ALL' and site !='ALL'  group by site""", db)

                    df_elect_src['year'] = year

                    df_elect_add = pd.read_sql(
                        f"""SELECT "year", site, total_elect FROM app.green_energy_amount where quarter = {quarter_num} and year = {year} and customer is not null and customer !='ALL' and customer !=''""", db)

                    df_solar_add = pd.read_sql(
                        f"""SELECT site, solar FROM app.green_energy_amount where quarter = {quarter_num} and year = {year} and customer is not null and customer !='ALL' and customer !=''""", db)

                    df_grelect_add = pd.read_sql(
                        f"""SELECT site, green_elect FROM app.green_energy_amount where quarter = {quarter_num} and year = {year} and customer is not null and customer !='ALL' and customer !=''""", db)

                    df_grenergy_add = pd.read_sql(
                        f"""SELECT site, green_energy FROM app.green_energy_amount where quarter = {quarter_num} and year = {year} and customer is not null and customer !='ALL' and customer !=''""", db)

                    df_grenergy_add['green_energy'] = 0

                    df_grenergy = pd.read_sql(
                        f"""SELECT site, sum(amount) as "green_energy"  FROM staging.renewable_energy_decarb where category = 'green_energy' and period_start >= '{period_start}' and period_start <= '{period_end}' and bo = 'ALL' and site !='ALL'  group by site""", db)

                    df_elect = pd.merge(df_elect_src, df_elect_add, on=[
                                        'year', 'site'], how='left')

                    df_elect = df_elect.fillna(0)

                    df_elect['total_elect'] = df_elect['total_elect_x'] - \
                        df_elect['total_elect_y']

                    df_elect = df_elect[['year', 'site', 'total_elect']]

                    df_solar = pd.merge(df_solar_src, df_solar_add, on=[
                                        'site'], how='left')

                    df_solar = df_solar.fillna(0)

                    df_solar['solar'] = df_solar['solar_x'] - df_solar['solar_y']

                    df_solar = df_solar[['site', 'solar']]

                    df_grelect = pd.merge(df_grelect_src, df_grelect_add, on=[
                                        'site'], how='left')

                    df_grelect = df_grelect.fillna(0)

                    df_grelect['green_elect'] = df_grelect['green_elect_x'] - \
                        df_grelect['green_elect_y']

                    df_grelect = df_grelect[['site', 'green_elect']]

                    df_renew_total = cal_renew_total(
                        renew_target, df_elect, df_solar, df_grelect, df_grenergy)

                    unit_price = pd.read_sql(
                        f"""SELECT "year", site,quarter, unit_price, amount as "actual_amount"  FROM app.green_purchase where customer ='-' or customer IS NULL""", db)

                    unit_price = unit_price[(unit_price['year'] == year) & (
                        unit_price['quarter'] == quarter)]

                    df_renew = pd.merge(df_renew_total, unit_price, on=[
                                        'site', 'year'], how='left')

                    df_renew['unit_price'] = df_renew['unit_price'].fillna(0)

                    df_renew['actual_amount'] = df_renew['actual_amount'].fillna(0)

                    df_renew['predict_price'] = df_renew['unit_price'] * \
                        df_renew['green_energy_request']

                    df_renew['ratio'] = df_renew['green_energy'] / \
                        df_renew['total_elect'] * 100

                    area_mapping = pd.read_sql(
                        f"""SELECT "year", site, area FROM staging.plant_mapping where year = {year}""", db)

                    area_mapping.drop_duplicates(inplace=True)

                    df_renew_area = pd.merge(df_renew, area_mapping, on=[
                                            'site', 'year'], how='left')

                    df_renew_area['quarter'] = quarter_num

                    df_renew_area['area'] = df_renew_area['area'].fillna('其他')

                    renew_customer_add = pd.read_sql(f"""SELECT "year", quarter, area, site, total_elect, target_renew, solar, green_elect, grey_elect, green_energy, predict_price,  green_energy_request, actual_amount
                                                                            FROM app.green_energy_amount where quarter = {quarter_num} and year = {year} and customer is not null and customer !='ALL' and customer !=''""", db)

                    df_renew_total = df_renew_area.append(renew_customer_add)

                    df_renew_total = df_renew_total.fillna(0)

                    df_renew_area_all = df_renew_total[['year', 'total_elect', 'grey_elect', 'target_renew', 'solar', 'green_elect',
                                                        'green_energy', 'green_energy_request', 'quarter', 'unit_price', 'predict_price', 'area', 'actual_amount']]

                    df_renew_area_all = df_renew_area_all.groupby(
                        ['year', 'quarter', 'area']).sum().reset_index()

                    df_renew_area_all['site'] = 'ALL'
                    df_renew_area_all['customer'] = 'ALL'

                    df_renew_area_all['ratio'] = df_renew_area_all['green_energy'] / \
                        df_renew_area_all['total_elect'] * 100

                    df_final = df_renew_area_all.append(df_renew_area)

                    df_final['customer'] = df_final['customer'].fillna('')

                    df_remark = pd.read_sql(f"""SELECT "year", quarter, area, site, customer, remark FROM app.green_energy_amount where year = {year} and quarter ={quarter_num}""",db)

                    df_final = pd.merge(df_final, df_remark, on=['year','quarter','site','area','customer'], how='left')



                    year_ = df_final['year']
                    quarter_ = df_final['quarter']
                    area_ = df_final['area']

                    if len(year_) == 0 or len(area_) == 0:

                        pass

                    elif df_final.size == 0:

                        pass

                    else:

                        delete_query = f"""DELETE FROM app.green_energy_amount WHERE (year IN {tuple(year_)} AND quarter IN {tuple(quarter_)} AND area IN {tuple(area_)} AND customer IS NULL) OR (year IN {tuple(year_)} AND quarter IN {tuple(quarter_)} AND area IN {tuple(area_)} AND customer ='ALL') OR (year IN {tuple(year_)} AND quarter IN {tuple(quarter_)} AND area IN {tuple(area_)} AND customer ='')  """

                        conn = db.connect()
                        conn.execute(delete_query)

                        df_final.to_sql('green_energy_amount', db, index=False,
                                        if_exists='append', schema='app', chunksize=10000)
                        conn.close()
    except Exception as inst:
        logger.exception("Exception ERROR => %s", str(inst))
