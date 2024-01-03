import calendar
from datetime import date
from datetime import datetime as dt
from datetime import timedelta

import numpy as np
import pandas as pd
from sqlalchemy import *

from models import engine
from services.mail_service import MailService


def cal_bo_site(data, category):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    plant_mapping = pd.read_sql('SELECT DISTINCT site FROM raw.plant_mapping WHERE boundary = true', con=db)

    #計算BO site
    data = data.merge(plant_mapping, on='site', how='inner')
#     data_copy = data.copy()
    # data_bo_all = data.copy()
    # data = data.append(data_bo_all)
    # site
    # if category == 0:
    #     data_site = data.groupby(
    #         ['bo', 'site', 'period_start']).sum().reset_index()

    # elif category == 1:
    #     data_site = data.groupby(
    #         ['bo', 'site', 'category', 'period_start']).sum().reset_index()

    # else:
    #     data_site = data.groupby(
    #         ['bo', 'site', 'category1', 'category2', 'period_start']).sum().reset_index()

    #     data_site['plant'] = 'ALL'

    data['bo'] = 'ALL'
    # bo
    if category == 0:
        data_bo = data.groupby(['bo', 'period_start']).sum().reset_index()

    elif category == 1:
        data_bo = data.groupby(
            ['bo', 'category', 'period_start']).sum().reset_index()

    else:
        data_bo = data.groupby(
            ['bo', 'category1', 'category2', 'period_start']).sum().reset_index()

    data_bo['site'] = 'ALL'

    # all
    data = data.append(data_bo).reset_index(drop=True)

    return data

def cal_site(data, category):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    # site
    if category == 0:
        data_site = data.groupby(
            ['site', 'period_start', 'meter_code', 'provider_name']).sum().reset_index()

    elif category == 1:
        data_site = data.groupby(
            ['site', 'category', 'period_start', 'meter_code', 'provider_name']).sum().reset_index()

    else:
        data_site = data.groupby(
            ['site', 'category1', 'category2', 'period_start', 'meter_code', 'provider_name']).sum().reset_index()
    data_site['plant'] = 'ALL'

    data_copy = data.copy()
    data_copy['site'] = 'ALL'

    # all
    data = data.append(data_site).append(data_copy).reset_index(drop=True)

    return data


def raw_to_staging(table_name, stage):

    table_name = str(table_name)

    # current_day = dt.now().day

    # if stage == 'development':  # DEV - 10號前抓2個月
    #     checkpoint = 10
    # else:  # PRD - 15號前抓2個月
    #     checkpoint = 12

    # set time - data in current year
    if dt.now().month == 1:

        year = dt.now().year-1
        month_start = 1
        month_end = 12
        period_year_start = date(dt.now().year-1, 1, 1).strftime("%Y-%m-%d")
        period_year_end = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")

    # elif (dt.now().month == 2) & (current_day < checkpoint):  # 10或15號前抓2個月前
        #  period_year_start = date(dt.now().year-1, 1, 1).strftime("%Y-%m-%d")
    #     period_start = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")

    #     period_start1 = date(dt.now().year-1, 12, 1).strftime("%Y%m")
    #     period_end = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")

    #     period = date(dt.now().year, dt.now().month-1, 1).strftime("%Y-%m")

    else:

        year = dt.now().year
        month_start = 1
        month_end = dt.now().month - 1
        period_year_start = date(dt.now().year, 1, 1).strftime("%Y-%m-%d")
        period_year_end = date(dt.now().year, 12, 1).strftime("%Y-%m-%d")

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    # 帳單為民國年
    # year = year-1911
    category_dict = {'尖峰': '經常尖峰', '周六半尖峰': '週六半尖峰'}
    # 電費帳單
    green_elect = pd.read_sql(
        f"""SELECT meter_code, category1, category2, amount, "year", "month" FROM raw.electric_bill where "year" = {year} and "month" >= {month_start} and "month" <= {month_end} """, db)

    green_elect = green_elect.replace({'category2': category_dict})
    # mapping表
    meter_mapping = pd.read_sql(
        f"""SELECT site, plant, code as "meter_code", elec_price_type FROM app.decarb_ww_site_elec_meter""", db)

    provider_mapping = pd.read_sql(
        f"""SELECT provider_name, code as "meter_code" FROM app.decarb_ww_meter_group """, db)

    provider_WHC = {'provider_name': '富威', 'meter_code': 'WHC_ALL'}
    provider_WNH = {'provider_name': '康舒', 'meter_code': 'WNH_ALL'}
    provider_mapping = provider_mapping.append(provider_WHC, ignore_index=True).append(provider_WNH, ignore_index=True).reset_index(drop=True)

    # 過濾表燈營業用電價
    green_elect = green_elect.merge(meter_mapping, on='meter_code', how='left')
    green_elect = green_elect[green_elect['elec_price_type'] != '表燈營業用電價']

    green_elect = green_elect[[
        'meter_code', 'category1', 'category2', 'amount', 'year', 'month']]
    meter_mapping = meter_mapping[['site', 'plant', 'meter_code']]

    WHC_ALL = {'site': 'WHC', 'plant': 'WHC', 'meter_code': 'WHC_ALL'}
    WNH_ALL = {'site': 'WNH', 'plant': 'WNH', 'meter_code': 'WNH_ALL'}
    meter_mapping = meter_mapping.append(WHC_ALL, ignore_index=True).append(WNH_ALL, ignore_index=True).reset_index(drop=True)

    # 民國改為西元
    # green_elect['year'] = green_elect['year'] + 1911
    green_elect['year'] = green_elect['year'].astype(str)
    green_elect['month'] = green_elect['month'].astype(str)

    green_elect['period_start'] = green_elect['year'] + \
        '-' + green_elect['month'] + '-01'
    green_elect['period_start'] = pd.to_datetime(
        green_elect['year'] + '-' + green_elect['month'] + '-01')
    green_elect = green_elect[[
        'meter_code', 'category1', 'category2', 'amount', 'period_start']]

    green_elect['Year'] = green_elect['period_start'].apply(lambda x: x.year)
    green_elect = green_elect.sort_values(
        by=['Year', 'meter_code', 'category1', 'category2', 'period_start'])
    green_elect['amount'] = green_elect['amount'].astype(float)
    green_elect['ytm_amount'] = green_elect.groupby(
        ['Year', 'meter_code', 'category1', 'category2'])['amount'].cumsum()
    green_elect = green_elect.drop('Year', axis=1).reset_index(drop=True)

    green_elect['meter_code'] = green_elect['meter_code'].astype(str)
    meter_mapping['meter_code'] = meter_mapping['meter_code'].astype(str)
    provider_mapping['meter_code'] = provider_mapping['meter_code'].astype(str)

    green_elect = green_elect.merge(meter_mapping, on='meter_code', how='left')
    green_elect = green_elect.merge(
        provider_mapping, on='meter_code', how='left')

    green_elect = cal_site(green_elect, 2)

    green_elect['last_update_time'] = dt.strptime(
        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

    if table_name == 'green_elect_contract':

        try:

            green_elect_contract = green_elect[green_elect['category1'] == '契約']

            if green_elect_contract.size != 0:

                conn = db.connect()
                conn.execute(
                    f"""DELETE FROM staging.green_elect_contract WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'""")
                green_elect_contract.to_sql('green_elect_contract', conn, index=False,
                                            if_exists='append', schema='staging', chunksize=10000)
                conn.close()

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] green_elect_contract etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'green_elect_price':

        try:

            green_elect_price = green_elect[green_elect['category1'] == '計費']

            if green_elect_price.size != 0:

                conn = db.connect()
                conn.execute(
                    f"""DELETE FROM staging.green_elect_price WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'""")
                green_elect_price.to_sql('green_elect_price', conn, index=False,
                                         if_exists='append', schema='staging', chunksize=10000)
                conn.close()

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] green_elect_price etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'grey_elect':

        try:

            grey_elect = green_elect[green_elect['category1'] == '需量']

            if green_elect.size != 0:

                conn = db.connect()
                conn.execute(
                    f"""DELETE FROM staging.grey_elect WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'""")
                grey_elect.to_sql('grey_elect', conn, index=False,
                                  if_exists='append', schema='staging', chunksize=10000)
                conn.close()

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] grey_elect etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'green_elect_vol':

        try:

            green_elect_vol = green_elect[green_elect['category1'] == '轉供']

            if green_elect_vol.size != 0:

                conn = db.connect()
                conn.execute(
                    f"""DELETE FROM staging.green_elect_vol WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'""")
                green_elect_vol.to_sql('green_elect_vol', conn, index=False,
                                       if_exists='append', schema='staging', chunksize=10000)
                conn.close()

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] green_elect_vol etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'solar':

        try:
            # 起始年份
            start_year = dt.now().year - 1
            # 结束年份（不包括）
            end_year = dt.now().year + 1

            for current_year in range(start_year, end_year):

                period_year_start = date(
                    current_year, 1, 1).strftime("%Y-%m-%d")

                if current_year == end_year - 1:

                    period_year_end = date(
                        current_year, 12, 1).strftime("%Y-%m-%d")

                else:

                    period_year_end = date(
                        current_year, 12, 1).strftime("%Y-%m-%d")

                solar_energy = pd.read_sql(
                    f"""SELECT plant,category,amount,period_start FROM raw.solar WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}'""", con=db)

                if solar_energy.shape[0] > 0:

                    solar_energy = solar_energy.fillna(0)

                    # 計算YTM
                    solar_energy['Year'] = solar_energy['period_start'].apply(
                        lambda x: x.year)
                    solar_energy = solar_energy.sort_values(
                        by=['Year', 'plant', 'category', 'period_start'])
                    solar_energy['ytm_amount'] = solar_energy.groupby(
                        ['Year', 'plant', 'category'])['amount'].cumsum()
                    solar_energy = solar_energy.drop('Year', axis=1)

                    plant_mapping = pd.read_sql(
                        'SELECT DISTINCT site,plant_name AS "plant" FROM raw.plant_mapping', con=db)
                    plant_mapping = plant_mapping.replace(
                        {'site': {'WKS': 'WKS', 'XTRKS': 'XTRKS'}})
                    df_solar = solar_energy.merge(
                        plant_mapping, on='plant', how='left')

                    # df_solar_site = df_solar.groupby(['site', 'category', 'period_start']).sum().reset_index()
                    # df_solar_site['plant'] = 'ALL'
                    # solar = df_solar.append(df_solar_site).reset_index(drop=True)
                    solar = df_solar.reset_index(drop=True)

                    solar = solar[['site', 'plant', 'category',
                                   'amount', 'ytm_amount', 'period_start']]
                    solar['last_update_time'] = dt.strptime(
                        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

                    conn = db.connect()
                    conn.execute(
                        f"""DELETE FROM staging.solar WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'""")
                    solar.to_sql('solar', conn, index=False,
                                 if_exists='append', schema='staging', chunksize=10000)
                    conn.close()

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] solar etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'solar_remain':

        try:

            # 起始年份
            start_year = dt.now().year - 1
            # 结束年份（不包括）
            end_year = dt.now().year + 1

            for current_year in range(start_year, end_year):

                period_year_start = date(
                    current_year, 1, 1).strftime("%Y-%m-%d")

                if current_year == end_year - 1:

                    if dt.now().month == 1:
                        period_year_end = date(current_year, 12, 1).strftime("%Y-%m-%d")

                    else:
                        period_year_end = date(current_year, date.today().month-1, 1).strftime("%Y-%m-%d")
                else:

                    period_year_end = date(
                        current_year, 12, 1).strftime("%Y-%m-%d")

                solar_remain_WKS = pd.read_sql(
                    f"""SELECT site ,amount,period_start FROM raw.solar_remain WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}' and site in ('WKS/XTRKS') """, con=db)
                WKS_ratio = pd.read_sql(
                    f"""SELECT ratio, plant, period_start FROM raw.fem_ratio_solar WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}' """, db)
                WKS_ratio['site'] = 'WKS/XTRKS'

                if solar_remain_WKS.shape[0] > 0:

                    solar_remain_WKS = solar_remain_WKS.merge(
                        WKS_ratio, on=['site', 'period_start'], how='left')
                    solar_remain_WKS['amount'] = solar_remain_WKS['amount'] * \
                        solar_remain_WKS['ratio']

                    solar_remain_WKS = solar_remain_WKS[[
                        'plant', 'period_start', 'amount']]

                solar_remain_WZS = pd.read_sql(
                    f"""SELECT site ,amount,period_start FROM raw.solar_remain WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}' and site in ('WZS') """, con=db)
                WZS_ratio = pd.read_sql(
                    f"""SELECT ratio, plant, period_start FROM raw.solar_ratio WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}' """, db)
                WZS_ratio['site'] = 'WZS'

                if solar_remain_WZS.shape[0] > 0:

                    solar_remain_WZS = solar_remain_WZS.merge(
                        WZS_ratio, on=['site', 'period_start'], how='left')
                    solar_remain_WZS['amount'] = solar_remain_WZS['amount'] * \
                        solar_remain_WZS['ratio']

                    solar_remain_WZS = solar_remain_WZS[[
                        'plant', 'period_start', 'amount']]

                solar_remain_other = pd.read_sql(
                    f"""SELECT site as plant ,amount,period_start FROM raw.solar_remain WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}'and site not in ('WZS','WKS/XTRKS') """, con=db)
                solar_remain_other = solar_remain_other.replace(
                    {'plant': {'WMI': 'WMI-2'}})
                solar_remain = solar_remain_other.append(
                    solar_remain_WZS).append(solar_remain_WKS)

                if solar_remain.shape[0] > 0:

                    solar_remain = solar_remain.fillna(0)

                    # 計算YTM
                    solar_remain['Year'] = solar_remain['period_start'].apply(
                        lambda x: x.year)
                    solar_remain = solar_remain.sort_values(
                        by=['Year', 'plant', 'period_start'])
                    solar_remain['ytm_amount'] = solar_remain.groupby(['Year', 'plant'])[
                        'amount'].cumsum()
                    solar_remain = solar_remain.drop('Year', axis=1)

                    plant_mapping = pd.read_sql(
                        'SELECT DISTINCT site,plant_name AS "plant" FROM raw.plant_mapping', con=db)
                    plant_mapping = plant_mapping.replace(
                        {'site': {'WKS': 'WKS', 'XTRKS': 'XTRKS'}})
                    df_remain = solar_remain.merge(
                        plant_mapping, on='plant', how='left')

                    # df_remain_site = df_remain.groupby(['site', 'period_start']).sum().reset_index()
                    # df_remain_site['plant'] = 'ALL'

                    # remain = df_remain.append(df_remain_site).reset_index(drop=True)
                    remain = df_remain.reset_index(drop=True)
                    remain = remain[['site', 'plant', 'amount',
                                     'ytm_amount', 'period_start']]
                    remain['last_update_time'] = dt.strptime(
                        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

                    conn = db.connect()
                    conn.execute(
                        f"""DELETE FROM staging.solar_remain WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'""")
                    remain.to_sql('solar_remain', conn, index=False,
                                  if_exists='append', schema='staging', chunksize=10000)
                    conn.close()

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] solar_remain etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'solar_other':

        try:

            area_dict = {'TB2': 'WZS-1', 'OB1': 'WZS-6',
                         'TB3': 'WZS-3', 'TB5': 'WZS-3'}
            # 起始年份
            start_year = dt.now().year - 1
            # 结束年份（不包括）
            end_year = dt.now().year + 1

            for current_year in range(start_year, end_year):

                period_year_start = date(
                    current_year, 1, 1).strftime("%Y-%m-%d")

                if current_year == end_year - 1:

                    if dt.now().month == 1:
                        period_year_end = date(current_year, 12, 1).strftime("%Y-%m-%d")

                    else:
                        period_year_end = date(current_year, date.today().month-1, 1).strftime("%Y-%m-%d")

                else:

                    period_year_end = date(
                        current_year, 12, 1).strftime("%Y-%m-%d")

                solar_other_WZS = pd.read_sql(
                    f"""SELECT period_start, site,area, tree, fuel FROM raw.solar_other where period_start >='{period_year_start}' and period_start <='{period_year_end}' and site in ('WZS') """, con=db)
                
                if solar_other_WZS.shape[0] > 0:
                    solar_other_WZS = solar_other_WZS.assign(plant=solar_other_WZS['area'].map(area_dict))[['period_start', 'plant', 'tree', 'fuel']].pivot_table(
                        index=['period_start', 'plant'], values=['tree', 'fuel'], aggfunc='sum').reset_index()
                    solar_other_WZS = pd.melt(solar_other_WZS, id_vars=[
                                            'period_start', 'plant'], var_name='category', value_name='amount')
                    solar_other_WZS = solar_other_WZS[[
                        'plant', 'period_start', 'amount', 'category']]

                solar_other = pd.read_sql(
                    f"""SELECT period_start, site as plant, tree, fuel FROM raw.solar_other where period_start >='{period_year_start}' and period_start <='{period_year_end}' and site not in ('WZS','WKS') """, con=db)
                if solar_other.shape[0] > 0:
                    solar_other = solar_other.groupby(
                        ['plant', 'period_start']).sum().reset_index()
                    solar_other = pd.melt(solar_other, id_vars=[
                                        'period_start', 'plant'], var_name='category', value_name='amount')
                    solar_other = solar_other.replace({'plant': {'WMI': 'WMI-2'}})
                    solar_other = solar_other[[
                        'plant', 'period_start', 'amount', 'category']]
                
                solar_other_WKS = pd.read_sql(
                        f"""SELECT period_start, site , tree, fuel FROM raw.solar_other where period_start >='{period_year_start}' and period_start <='{period_year_end}' and site in ('WKS') """, con=db)
                if solar_other_WKS.shape[0] > 0:    
                    solar_other_WKS = solar_other_WKS.groupby(
                        ['site', 'period_start']).sum().reset_index()
                    solar_other_WKS = pd.melt(solar_other_WKS, id_vars=[
                                            'period_start', 'site'], var_name='category', value_name='amount')

                    WKS_ratio = pd.read_sql(
                        f"""SELECT ratio, plant, period_start FROM raw.fem_ratio_solar WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}' """, db)
                    WKS_ratio['site'] = 'WKS'

                    solar_other_WKS = solar_other_WKS.merge(
                        WKS_ratio, on=['site', 'period_start'], how='left')
                    solar_other_WKS['amount'] = solar_other_WKS['amount'] * \
                        solar_other_WKS['ratio']
                    solar_other_WKS = solar_other_WKS[[
                        'plant', 'period_start', 'amount', 'category']]

                solar_other_total = solar_other.append(
                    solar_other_WZS).append(solar_other_WKS)

                solar_other_total = solar_other_total.fillna(0)
                if solar_other_total.shape[0] > 0:  
                    # 計算YTM
                    solar_other_total['Year'] = solar_other_total['period_start'].apply(
                        lambda x: x.year)
                    solar_other_total = solar_other_total.sort_values(
                        by=['Year', 'plant', 'category', 'period_start'])
                    solar_other_total['ytm_amount'] = solar_other_total.groupby(
                        ['Year', 'plant', 'category'])['amount'].cumsum()
                    solar_other_total = solar_other_total.drop('Year', axis=1)

                    plant_mapping = pd.read_sql(
                        'SELECT DISTINCT site,plant_name AS "plant" FROM raw.plant_mapping', con=db)
                    plant_mapping = plant_mapping.replace(
                        {'site': {'WKS': 'WKS', 'XTRKS': 'XTRKS'}})
                    df_solar_other = solar_other_total.merge(
                        plant_mapping, on='plant', how='left')

                    # df_solar_other_site = df_solar_other.groupby(['site', 'category', 'period_start']).sum().reset_index()
                    # df_solar_other_site['plant'] = 'ALL'
                    # solar_other = df_solar_other.append(df_solar_other_site).reset_index(drop=True)
                    solar_other = df_solar_other.reset_index(drop=True)

                    solar_other = solar_other[[
                        'site', 'plant', 'category', 'amount', 'ytm_amount', 'period_start']]
                    solar_other['last_update_time'] = dt.strptime(
                        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

                    conn = db.connect()
                    conn.execute(
                        f"""DELETE FROM staging.solar_other WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'""")
                    solar_other.to_sql('solar_other', conn, index=False,
                                    if_exists='append', schema='staging', chunksize=10000)
                    conn.close()

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] solar_other etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'solar_info':

        try:

            if dt.now().month == 1:

                period_start = date(dt.now().year-1, 12,
                                    1).strftime("%Y-%m-%d")

            else:

                period_start = date(
                    dt.now().year, dt.now().month - 1, 1).strftime("%Y-%m-%d")

            solar_info_WKS = pd.read_sql(
                f"""SELECT * FROM raw.solar_info where site in ('WKS') """, con=db)

            # solar_info_WKS_all = solar_info_WKS.copy()

            # solar_info_WKS_all['site'] = 'WKS/XTRKS'
            # solar_info_WKS_all['plant'] = 'ALL'
            # solar_info_WKS_all['ytm_amount'] = solar_info_WKS_all['amount']
            # solar_info_WKS_all['period_start'] = period_start
            # solar_info_WKS_all.drop_duplicates(inplace=True)
            # solar_info_WKS_all = solar_info_WKS_all[['plant', 'category', 'amount', 'site', 'period_start', 'ytm_amount']]

            WKS_ratio = pd.read_sql(
                f"""SELECT ratio, plant, period_start FROM raw.fem_ratio_solar WHERE period_start ='{period_start}' """, db)
            solar_info_WKS = solar_info_WKS.merge(
                WKS_ratio, on='plant', how='left')
            solar_info_WKS['amount'] = solar_info_WKS['amount'] * \
                solar_info_WKS['ratio']
            solar_info_WKS['ytm_amount'] = solar_info_WKS['amount']
            solar_info_WKS['site'] = 'WKS/XTRKS'
            solar_info_WKS = solar_info_WKS[[
                'plant', 'category', 'amount', 'site', 'period_start', 'ytm_amount']]

            solar_info_XTRKS = solar_info_WKS[solar_info_WKS['plant'] == 'XTRKS']
            solar_info_WKS_1 = solar_info_WKS[solar_info_WKS['plant'] != 'XTRKS']

            solar_info_XTRKS['site'] = 'XTRKS'
            solar_info_WKS_1['site'] = 'WKS'

            solar_info_other = pd.read_sql(
                f"""SELECT * FROM raw.solar_info where site not in ('WKS','WZS') """, con=db)

            # solar_info_other_all = solar_info_other.copy()

            # solar_info_other_all['plant'] = 'ALL'
            # solar_info_other_all['ytm_amount'] = solar_info_other_all['amount']
            # solar_info_other_all['period_start'] = period_start
            # solar_info_other_all.drop_duplicates(inplace=True)
            # solar_info_other_all = solar_info_other_all[['plant', 'category', 'amount', 'site', 'period_start', 'ytm_amount']]

            solar_info_other['ytm_amount'] = solar_info_other['amount']
            solar_info_other['period_start'] = period_start
            solar_info_other = solar_info_other[[
                'plant', 'category', 'amount', 'site', 'period_start', 'ytm_amount']]

            solar_info_WZS = pd.read_sql(
                f"""SELECT * FROM raw.solar_info where site in ('WZS') """, con=db)

            # solar_info_WZS_all = solar_info_WZS.copy()

            # solar_info_WZS_all['plant'] = 'ALL'
            # solar_info_WZS_all = solar_info_WZS_all.groupby(['site','plant','category']).sum().reset_index()
            # solar_info_WZS_all['ytm_amount'] = solar_info_WZS_all['amount']
            # solar_info_WZS_all['period_start'] = period_start
            # solar_info_WZS_all = solar_info_WZS_all[['plant', 'category', 'amount', 'site', 'period_start', 'ytm_amount']]

            solar_info_WZS['ytm_amount'] = solar_info_WZS['amount']
            solar_info_WZS['period_start'] = period_start
            solar_info_WZS = solar_info_WZS[[
                'plant', 'category', 'amount', 'site', 'period_start', 'ytm_amount']]

            solar_info = solar_info_WKS_1.append(solar_info_XTRKS).append(
                solar_info_other).append(solar_info_WZS).reset_index(drop=True)

            solar_info['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn = db.connect()
            conn.execute(
                f"""DELETE FROM staging.solar_info WHERE  period_start ='{period_start}'""")
            solar_info.to_sql('solar_info', conn, index=False,
                              if_exists='append', schema='staging', chunksize=10000)
            conn.close()

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] solar_info etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'elect_total':

        try:

            if dt.now().month == 1:

                year = dt.now().year-1
                month_start = 1
                month_end = 12
                period_year_start = date(
                    dt.now().year-1, 1, 1).strftime("%Y-%m-%d")
                period_year_end = date(
                    dt.now().year-1, 12, 1).strftime("%Y-%m-%d")

            else:

                year = dt.now().year
                month_start = 1
                month_end = 12
                period_year_start = date(
                    dt.now().year, 1, 1).strftime("%Y-%m-%d")
                period_year_end = date(
                    dt.now().year, 12, 1).strftime("%Y-%m-%d")

            # elect_target
            # elect_target = pd.read_sql(f"""SELECT site, "year", "month",  amount FROM app.decarb_elect_summary where "year" = {year} and "month" >= {month_start} and "month" <= {month_end} and "version" = (SELECT MAX("version") FROM app.decarb_elect_summary)""", db)
            elect_target = pd.read_sql(
                f"""SELECT site, "month", amount, "year" FROM app.elect_target_month  where "year" = {year} and "month" >= {month_start} and "month" <= {month_end} and category = 'predict' and site !='All' and "version" = (SELECT MAX("version") FROM app.elect_target_month where "year" = {year} and validate is true) """, db)
            elect_target['year'] = elect_target['year'].astype(str)
            elect_target['month'] = elect_target['month'].astype(str)

            elect_target['period_start'] = elect_target['year'] + \
                '-' + elect_target['month'] + '-01'
            elect_target['period_start'] = pd.to_datetime(
                elect_target['year'] + '-' + elect_target['month'] + '-01')
            elect_target['period_start'] = elect_target['period_start'].astype(
                str)

            elect_target = elect_target[['site', 'period_start', 'amount']]

            # elect_target_WKS
            elect_target_WKS = elect_target[elect_target['site'].isin(['WKS'])]

            WKS_ratio = pd.read_sql(
                f"""SELECT ratio, plant , period_start FROM raw.fem_ratio WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}' and category ='plant' """, db)
            WKS_ratio['site'] = 'WKS'
            WKS_ratio['period_start'] = WKS_ratio['period_start'].astype(str)

            elect_target_WKS = elect_target_WKS.merge(
                WKS_ratio, on=['site', 'period_start'], how='left')

            elect_target_WKS['amount'] = elect_target_WKS['amount'] * \
                elect_target_WKS['ratio']

            elect_target_WKS = elect_target_WKS[[
                'plant', 'period_start', 'amount']]

            # elect_target_WZS
            elect_target_WZS = elect_target[elect_target['site'].isin(['WZS'])]

            WZS_ratio = pd.read_sql(
                f"""SELECT ratio, plant, period_start FROM raw.solar_ratio WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}' """, db)
            WZS_ratio['site'] = 'WZS'
            WZS_ratio['period_start'] = WZS_ratio['period_start'].astype(str)

            elect_target_WZS = elect_target_WZS.merge(
                WZS_ratio, on=['site', 'period_start'], how='left')

            elect_target_WZS['amount'] = elect_target_WZS['amount'] * \
                elect_target_WZS['ratio']

            elect_target_WZS = elect_target_WZS[[
                'plant', 'period_start', 'amount']]

            # elect_target_other
            elect_target_other = elect_target[~elect_target['site'].isin([
                                                                         'WZS', 'WKS'])]
            elect_target_other.rename(columns={'site': 'plant'}, inplace=True)

            # elect_target_total
            elect_target_total = elect_target_other.append(
                elect_target_WZS).append(elect_target_WKS).reset_index(drop=True)

            # elect_actual
            elect_actual = pd.read_sql(
                f"""SELECT site, "month", amount, "year" FROM app.elect_target_month  where "year" = {year} and "month" >= {month_start} and "month" <= {month_end} and category = 'actual' and site !='All' and "version" = (SELECT MAX("version") FROM app.elect_target_month where "year" = {year} and validate is true)""", db)

            elect_actual['year'] = elect_actual['year'].astype(str)
            elect_actual['month'] = elect_actual['month'].astype(str)

            elect_actual['period_start'] = elect_actual['year'] + \
                '-' + elect_actual['month'] + '-01'
            elect_actual['period_start'] = pd.to_datetime(
                elect_actual['year'] + '-' + elect_actual['month'] + '-01')
            elect_actual['period_start'] = elect_actual['period_start'].astype(
                str)

            elect_actual = elect_actual[['site', 'period_start', 'amount']]

            # elect_actual_WKS #elect_actual_WZS
            elect_actual_WKS = elect_actual[elect_actual['site'].isin(['WKS'])]
            elect_actual_WZS = elect_actual[elect_actual['site'].isin(['WZS'])]

            elect_actual_WKS = elect_actual_WKS.merge(
                WKS_ratio, on=['site', 'period_start'], how='left')

            elect_actual_WKS['amount'] = elect_actual_WKS['amount'] * \
                elect_actual_WKS['ratio']

            elect_actual_WKS = elect_actual_WKS[[
                'plant', 'period_start', 'amount']]

            elect_actual_WZS = elect_actual_WZS.merge(
                WZS_ratio, on=['site', 'period_start'], how='left')

            elect_actual_WZS['amount'] = elect_actual_WZS['amount'] * \
                elect_actual_WZS['ratio']

            elect_actual_WZS = elect_actual_WZS[[
                'plant', 'period_start', 'amount']]

            # elect_actual_other
            elect_actual_other = elect_actual[~elect_actual['site'].isin([
                                                                         'WZS', 'WKS'])]
            elect_actual_other.rename(columns={'site': 'plant'}, inplace=True)

            # elect_actual_total
            elect_actual_total = elect_actual_other.append(
                elect_actual_WZS).append(elect_actual_WKS).reset_index(drop=True)

            # elect_total
            elect_target_total['category'] = 'target'
            elect_actual_total['category'] = 'actual'
            elect_total = elect_target_total.append(
                elect_actual_total).reset_index(drop=True)

            # 計算YTM
            elect_total = elect_total.fillna(0)
            elect_total['period_start'] = pd.to_datetime(
                elect_total['period_start'])
            elect_total['Year'] = elect_total['period_start'].dt.year
            elect_total = elect_total.sort_values(
                by=['Year', 'plant', 'category', 'period_start'])
            elect_total['ytm_amount'] = elect_total.groupby(
                ['Year', 'plant', 'category'])['amount'].cumsum()
            elect_total = elect_total.drop('Year', axis=1)

            # 關聯site
            plant_mapping = pd.read_sql(
                'SELECT DISTINCT site,plant_name AS "plant" FROM raw.plant_mapping', con=db)
            plant_mapping = plant_mapping.replace(
                {'site': {'WKS': 'WKS', 'XTRKS': 'XTRKS'}})
            elect_total = elect_total.merge(
                plant_mapping, on='plant', how='left')
            elect_total = elect_total.reset_index(drop=True)

            elect_total = elect_total[[
                'site', 'plant', 'category', 'amount', 'ytm_amount', 'period_start']]
            elect_total['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn = db.connect()
            conn.execute(
                f"""DELETE FROM staging.elect_total WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'""")
            elect_total.to_sql('elect_total', conn, index=False,
                               if_exists='append', schema='staging', chunksize=10000)
            conn.close()

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] elect_total etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'provider_plant_list':

        try:

            if dt.now().month == 1:

                year = dt.now().year-1
                month_start = 1
                month_current = dt.now().month
                month_end = 12

                period_year_start = date(
                    dt.now().year-1, 1, 1).strftime("%Y-%m-%d")
                period_end = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")

            else:

                year = dt.now().year
                month_start = 1
                month_current = dt.now().month-1
                month_end = 12

                period_year_start = date(
                    dt.now().year, 1, 1).strftime("%Y-%m-%d")
                period_end = date(
                    dt.now().year, dt.now().month-1, 1).strftime("%Y-%m-%d")

            # provider_target = pd.read_sql(f"""SELECT  country, area, "year", "month", provider, site, amount FROM app.provider_plant_list where "year" = {year} and "month" >= {month_start} and "month" <= {month_current}""", db)

            provider_target = pd.read_sql(
                f"""SELECT  area, "year", "month", provider, site, amount FROM app.provider_plant_list where "year" = {year} and "month" >= {month_start} and "month" <= {month_current}""", db)

            provider_target['year'] = provider_target['year'].astype(str)
            provider_target['month'] = provider_target['month'].astype(str)

            provider_target['period_start'] = provider_target['year'] + \
                '-' + provider_target['month'] + '-01'
            provider_target['period_start'] = pd.to_datetime(
                provider_target['year'] + '-' + provider_target['month'] + '-01')

            # provider_target = provider_target[['country', 'area', 'year', 'provider', 'site', 'amount', 'period_start']]
            provider_target = provider_target[[
                'area', 'year', 'provider', 'site', 'amount', 'period_start']]

            # provider_target = provider_target.sort_values(by=['year', 'country', 'area', 'site','provider','period_start'])
            provider_target = provider_target.sort_values(
                by=['year', 'area', 'site', 'provider', 'period_start'])

            provider_target['amount'] = provider_target['amount'].astype(float)

            # provider_target['ytm_amount'] = provider_target.groupby(['year', 'country', 'area', 'site','provider'])['amount'].cumsum()
            provider_target['ytm_amount'] = provider_target.groupby(
                ['year', 'area', 'site', 'provider'])['amount'].cumsum()

            provider_target = provider_target.drop(
                'year', axis=1).reset_index(drop=True)

            provider_target['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            if provider_target.size != 0:

                conn = db.connect()
                conn.execute(
                    f"""DELETE FROM staging.provider_plant_list WHERE  period_start >='{period_year_start}' and period_start <='{period_end}'""")
                provider_target.to_sql('provider_plant_list', conn, index=False,
                                       if_exists='append', schema='staging', chunksize=10000)
                conn.close()

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] provider_plant_list etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'electricity_decarb':

        try:

            # electricity = pd.read_sql(f"""SELECT site,amount,period_start FROM raw.electricity_total_decarb WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}'""", con=db)

            '''
            electricity_origin + renew_soalr @11/24
            將 (來源為CSR) 用電 + (來源為esgi) 太陽能 : CSR 用電 + wzs_esgi 太陽能
            將 (來源為esgi)用電維持,  不要關聯到太陽能 : wzs_esgi 用電 + 太陽能.fillna(0)
            故將type改名為CSR'在依照'site','period_start','type'進行關聯
            '''
            electricity_origin = pd.read_sql(f"""SELECT site,amount,period_start,type FROM raw.electricity_total_decarb WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}'""", con=db)

            electricity_origin['amount'] = electricity_origin['amount'].fillna(0)

            renew_solar = pd.read_sql(f"""SELECT site, amount, period_start FROM raw.renewable_energy_decarb where period_start >='{period_year_start}' and period_start <='{period_year_end}' and category2 = '光伏'""",con=db)

            renew_solar['type'] = 'CSR'

            electricity_actual = electricity_origin.merge(renew_solar, on=['site','period_start','type'], how='left')

            electricity_actual['amount_y'] = electricity_actual['amount_y'].fillna(0)

            electricity_actual['amount'] = electricity_actual['amount_x'] + electricity_actual['amount_y']

            electricity = electricity_actual[['site','amount','period_start']]
            # 計算YTM
            electricity['Year'] = electricity['period_start'].apply(lambda x: x.year)
            electricity = electricity.sort_values( by=['Year', 'site', 'period_start'])
            electricity['ytm_amount'] = electricity.groupby(['Year', 'site'])['amount'].cumsum()
            electricity = electricity.drop('Year', axis=1)

            #計算bo and site
            electricity = cal_bo_site(electricity, 0)
            electricity = electricity[[
                'bo', 'site', 'amount', 'ytm_amount', 'period_start']]
            electricity['unit'] = "度"
            electricity['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")


            if electricity.size != 0:

                conn = db.connect()
                conn.execute(
                    f"""DELETE FROM staging.electricity_decarb WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'""")
                electricity.to_sql('electricity_decarb', conn, index=False,
                                if_exists='append', schema='staging', chunksize=10000)
                conn.close()

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] decarb electricity raw2stage etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error


    if table_name == 'renewable_energy_decarb':

        try:

            renewable_energy = pd.read_sql(f"""SELECT site,category2 AS "category",amount,period_start FROM raw.renewable_energy_decarb WHERE category1 = '綠色能源' AND period_start >='{period_year_start}' and period_start <='{period_year_end}'""", con=db)

            if renewable_energy.shape[0] > 0:

                renewable_energy.loc[renewable_energy['category']
                                    == '光伏', 'category'] = 'solar_energy'
                renewable_energy.loc[renewable_energy['category']
                                    == '綠證', 'category'] = 'green_energy'
                renewable_energy.loc[renewable_energy['category']
                                    == '綠電', 'category'] = 'green_electricity'
                renewable_energy.loc[renewable_energy['site']
                                    == 'WKS-P6A', 'site'] = 'WKS-6A'
                renewable_energy.loc[renewable_energy['site']
                                    == 'WKS-P6B', 'site'] = 'WKS-6B'
                renewable_energy.loc[renewable_energy['site']
                                    == 'WKS-P6', 'site'] = 'WKS-6'
                renewable_energy = renewable_energy.fillna(0)

                # 計算YTM
                renewable_energy['Year'] = renewable_energy['period_start'].apply(
                    lambda x: x.year)
                renewable_energy = renewable_energy.sort_values(
                    by=['Year', 'site', 'category', 'period_start'])
                renewable_energy['ytm_amount'] = renewable_energy.groupby(
                    ['Year', 'site', 'category'])['amount'].cumsum()
                renewable_energy = renewable_energy.drop('Year', axis=1)

                #計算bo and site
                renewable_energy = cal_bo_site(renewable_energy, 1)
                renewable_energy = renewable_energy[[
                    'bo', 'site', 'category', 'amount', 'ytm_amount', 'period_start']]
                renewable_energy['unit'] = "度"
                renewable_energy['last_update_time'] = dt.strptime(
                    dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")



                if renewable_energy.size != 0:

                    conn = db.connect()
                    conn.execute(
                        f"DELETE FROM staging.renewable_energy_decarb WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'")
                    renewable_energy.to_sql('renewable_energy_decarb', conn, index=False,
                                            if_exists='append', schema='staging', chunksize=10000)
                    conn.close()

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] decarb renewable_energy raw2stage etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error
