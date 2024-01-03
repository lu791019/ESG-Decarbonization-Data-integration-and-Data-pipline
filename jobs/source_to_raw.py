import calendar
import http.client
import json
from base64 import b64encode
from datetime import date
from datetime import datetime as dt
from datetime import timedelta

import numpy as np
import pandas as pd
import requests
import urllib3
from dateutil.relativedelta import relativedelta
from sqlalchemy import *

from models import engine
from services.mail_service import MailService

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

connect_eco_string = engine.get_connect_string()
db_eco = create_engine(connect_eco_string, echo=True)


def fem_ratio_cal(site, consumtype, start_time, end_time):

    FEM_elect = pd.read_sql(
        f"""SELECT plant AS "plant_code", datadate, power FROM raw.wks_mfg_fem_dailypower where site in ('{site}') and datadate >= '{start_time}' and  datadate<= '{end_time}' and consumetype = '{consumtype}'""", db_eco)
    plant_map = pd.read_sql(
        f"""SELECT DISTINCT site,plant_name AS "plant",plant_code FROM raw.plant_mapping where site in ('{site}')""", con=db_eco)

    df = FEM_elect.merge(plant_map, on='plant_code', how='left').dropna()
    df['power'] = df.groupby(['plant'])['power'].transform('sum')
    df.drop(['datadate'], axis=1, inplace=True)
    df.drop_duplicates(inplace=True)
    df['ratio'] = df['power'].div(df['power'].sum())
    df['period_start'] = str(start_time)
    df.rename(columns={'power': 'amount'}, inplace=True)

    return df


def solar_month(type_, site, start_time):
    # NOTE: InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly advised.
    re_json = requests.get(
        f"https://pps-api.wzs-arm-prd-02.k8s.wistron.com/power-generation-month/getPowerGenerationForWigps/{type_}/{site}",
        verify=False).json()
    period_start = pd.Series(re_json['datetime'], name='period_start')

    actual_amount = pd.Series(re_json['actual'], name='amount')
    df = pd.concat([actual_amount, period_start], axis=1)
    df['period_start'] = df['period_start'].apply(
        lambda x: dt.strptime(x, "%Y-%m").strftime("%Y-%m-%d"))
    # df = df[df['period_start'] <= start_time]
    df['plant'] = site
    df['category'] = 'actual'

    target_amount = pd.Series(re_json['target'], name='amount')
    target_df = pd.concat([target_amount, period_start], axis=1)
    target_df['period_start'] = target_df['period_start'].apply(
        lambda x: dt.strptime(x, "%Y-%m").strftime("%Y-%m-%d"))
    # target_df = target_df[target_df['period_start'] <= start_time]
    target_df['plant'] = site
    target_df['category'] = 'target'

    df = df.append(target_df)

    return df


def source_to_raw(table_name, stage):

    if table_name == 'fem_ratio':

        if dt.now().month == 1:
            start_time = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
            end_time = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")

        else:
            start_time = date(dt.now().year, dt.now().month -
                              1, 1).strftime("%Y-%m-%d")
            end_time = date(dt.now().year, dt.now().month-1,
                            calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")

        try:

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

                delete_query = f"""DELETE FROM raw.fem_ratio WHERE plant IN {tuple(plant)} AND period_start IN {tuple(period_start)} AND category IN {tuple(category)}"""

                conn = db_eco.connect()
                conn.execute(delete_query)

                df.to_sql('fem_ratio', con=db_eco, schema='raw',
                          if_exists='append', index=False, chunksize=1000)
                conn.close()

            return True

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] source2raw etl:fem_ratio info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'fem_ratio_solar':

        if dt.now().month == 1:
            start_time = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
            end_time = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")

        else:
            start_time = date(dt.now().year, dt.now().month -
                              1, 1).strftime("%Y-%m-%d")
            end_time = date(dt.now().year, dt.now().month-1,
                            calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")

        try:

            site = 'WKS'
            consumtype = '用電量'

            FEM_elect = pd.read_sql(
                f"""SELECT plant AS "plant_code", datadate, power FROM raw.wks_mfg_fem_dailypower where site in ('{site}') and datadate >= '{start_time}' and  datadate<= '{end_time}' and consumetype = '{consumtype}'""", db_eco)
            plant_map = pd.read_sql(
                f"""SELECT DISTINCT site,plant_name AS "plant",plant_code FROM raw.plant_mapping where site in ('{site}','XTRKS')""", con=db_eco)
            df = FEM_elect.merge(
                plant_map, on='plant_code', how='left').dropna()
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

                conn = db_eco.connect()
                conn.execute(delete_query)

                df.to_sql(str(table_name), con=db_eco, schema='raw',
                          if_exists='append', index=False, chunksize=1000)
                conn.close()

            return True

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] source2raw etl:fem_ratio_solar info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'solar_ratio':

        if dt.now().month == 1:
            start_time = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
            end_time = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")

        else:
            start_time = date(dt.now().year, dt.now().month -
                              1, 1).strftime("%Y-%m-%d")
            end_time = date(dt.now().year, dt.now().month-1,
                            calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")

        try:

            period_start = start_time
            WZS_solar = pd.read_sql(
                f"""SELECT plant, amount, period_start FROM raw.renewable_energy where period_start = '{period_start}' and category1 ='綠色能源' and category2 ='光伏' and plant in ('WZS-1','WZS-3','WZS-6','WZS-8')""", db_eco)
            WZS_solar['ratio'] = WZS_solar['amount'].div(
                WZS_solar['amount'].sum())

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

                conn = db_eco.connect()
                conn.execute(delete_query)

                WZS_solar.to_sql(str(table_name), con=db_eco, schema='raw',
                                 if_exists='append', index=False, chunksize=1000)
                conn.close()

            return True

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] source2raw etl:solar_ratio info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'solar':

        if dt.now().month == 1:
            start_time = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
            end_time = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")

        else:
            start_time = date(dt.now().year, dt.now().month -
                              1, 1).strftime("%Y-%m-%d")
            end_time = date(dt.now().year, dt.now().month-1,
                            calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")
        try:

            df_WZS1 = solar_month('area', 'TB2', start_time)
            df_WZS1['plant'] = 'WZS-1'

            df_WZS6 = solar_month('area', 'OB1', start_time)
            df_WZS6['plant'] = 'WZS-6'

            df_TB3 = solar_month('area', 'TB3', start_time)
            df_TB3['plant'] = 'WZS-3'

            df_TB5 = solar_month('area', 'TB5', start_time)
            df_TB5['plant'] = 'WZS-3'

            df_WZS3 = df_TB3.append(df_TB5)
            df_WZS3 = df_WZS3.groupby(
                ['plant', 'period_start', 'category']).sum().reset_index()

            df_WVN = solar_month('site', 'WVN', start_time)
            df_WOK = solar_month('site', 'WOK', start_time)

            df_WNH = solar_month('site', 'WHC', start_time)
            df_WNH = df_WNH.replace({'plant': {'WHC': 'WNH'}})

            df_WMI = solar_month('site', 'WMI', start_time)
            df_WMI = df_WMI.replace({'plant': {'WMI': 'WMI-2'}})

            df_WKS = solar_month('site', 'WKS', start_time)
            df_WKS.rename(columns={'plant': 'site',
                          'amount': 'power'}, inplace=True)

            fem_ratio = pd.read_sql(
                f"""SELECT ratio, plant, period_start FROM raw.fem_ratio_solar""", db_eco)
            fem_ratio['site'] = 'WKS'

            df_WKS['period_start'] = df_WKS['period_start'].astype(str)
            fem_ratio['period_start'] = fem_ratio['period_start'].astype(str)

            df_WKS = pd.merge(df_WKS, fem_ratio, on=[
                              'period_start', 'site'], how='left')
            df_WKS['amount'] = df_WKS['power'] * df_WKS['ratio']
            df_WKS = df_WKS[['period_start', 'plant', 'amount', 'category']]

            solar = df_WKS.append(df_WZS1).append(df_WZS3).append(df_WZS6).append(
                df_WVN).append(df_WOK).append(df_WNH).append(df_WMI)

            solar['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            solar.dropna(inplace=True)
            solar.drop_duplicates(inplace=True)

            period_start = solar['period_start']
            plant = solar['plant']
            category = solar['category']

            if len(period_start) == 0 or len(plant) == 0:

                pass

            elif solar.size == 0:

                pass

            else:

                delete_query = f"""DELETE FROM raw.solar WHERE plant IN {tuple(plant)} AND period_start IN {tuple(period_start)} AND category IN {tuple(category)} """

                conn = db_eco.connect()
                conn.execute(delete_query)

                solar.to_sql(str(table_name), con=db_eco, schema='raw',
                             if_exists='append', index=False, chunksize=1000)
                conn.close()

            return True

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] source2raw etl:solar info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error
