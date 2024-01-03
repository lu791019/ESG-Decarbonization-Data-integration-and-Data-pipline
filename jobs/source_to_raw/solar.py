from datetime import datetime as dt

import pandas as pd
import requests
import urllib3

from app.logger import logger
from helpers.decarb_date import DecarbDate
from models import engine
from services.mail_service import MailService

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def main():
    start_time = DecarbDate.start_time()
    end_time = DecarbDate.end_time()
    logger.info('fem_ratio start_time: %s, end_time: %s', start_time, end_time)

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
    df_WKS.rename(columns={'plant': 'site', 'amount': 'power'}, inplace=True)

    fem_ratio = engine.pd_read_sql(
        f"""SELECT ratio, plant, period_start FROM raw.fem_ratio_solar""")
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

        delete_query = f"""DELETE FROM raw.solar WHERE plant IN {tuple(plant)} AND period_start IN {tuple(period_start)} AND category IN {tuple(category)}"""
        engine.execute_sql(delete_query)

        table_name = 'solar'
        engine.pd_to_sql(str(table_name), solar, schema='raw',
                         if_exists='append', index=False, chunksize=1000)

    return True


def getDataFromWzsArmPrd(type_, site):
    try:
        # NOTE: InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly advised.
        result = requests.get(
            f"https://pps-api.wzs-arm-prd-02.k8s.wistron.com/power-generation-month/getPowerGenerationForWigps/{type_}/{site}",
            verify=False, timeout=30).json()
        if result['datetime'] == []:
            MailService('[failed] solor ETL error', [
                'Vincent_ku@wistron.com',
                'Dex_Lu@wistron.com',
                'Felix_ye@wistron.com',
            ]).send_text('data from wzs arm prd wzs datetime is empty.')

        return result
    except Exception as error:  # pylint: disable=broad-except
        raise Exception(f"getDataFromWzsArmPrd error:") from error


def solar_month(type_, site, start_time):
    re_json = getDataFromWzsArmPrd(type_, site)

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
