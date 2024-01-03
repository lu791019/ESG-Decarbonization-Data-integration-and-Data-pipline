import calendar
from datetime import date
from datetime import datetime as dt
from datetime import timedelta

import numpy as np
import pandas as pd
from sqlalchemy import *

from models import engine
from services.mail_service import MailService


def green_energy_target_adjust(green_energy_target_all, green_energy_target_filter):

    green_energy_target_all['amount'] = green_energy_target_all['ytm_amount']
    green_energy_target_all = green_energy_target_all.merge(
        green_energy_target_filter, how='cross')
    if green_energy_target_all.size != 0:
        green_energy_target_all['amount'] = green_energy_target_all['amount_x'] - \
            green_energy_target_all['amount_y']

    green_energy_target_all_fix = green_energy_target_all[['period_start', 'ytm_amount', 'amount']]

    return green_energy_target_all_fix


def coef_preprocess(year, site_dict):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    coef = pd.read_sql(
        f"""SELECT site , amount as coef FROM staging.cfg_carbon_coef where "year" = {year} """, db)
    coef['site'] = coef['site'].replace(site_dict)
    coef.drop_duplicates(inplace=True)

    return coef


def s2_market_cal(df_elect, renw_market, coef):

    scope2_market = pd.merge(pd.merge(df_elect, renw_market, on=[
                             'site', 'period_start'], how='left'),  coef, on=['site'], how='left')
    scope2_market = scope2_market.fillna(0)
    if scope2_market.size != 0:
        scope2_market['amount'] = (
            scope2_market['amount_x'] - scope2_market['amount_y'])*scope2_market['coef'] / 1000
        scope2_market['ytm_amount'] = (
            scope2_market['ytm_amount_x'] - scope2_market['ytm_amount_y'])*scope2_market['coef'] / 1000
        scope2_market = scope2_market[['period_start', 'amount', 'ytm_amount']]
        scope2_market_all = scope2_market.groupby(
            ['period_start']).sum().reset_index()
    else:
        scope2_market['amount'] = (
            scope2_market['amount_x'] - scope2_market['amount_y'])*scope2_market['coef']
        scope2_market['ytm_amount'] = (
            scope2_market['ytm_amount_x'] - scope2_market['ytm_amount_y'])*scope2_market['coef']
        scope2_market_all = scope2_market[[
            'period_start', 'amount', 'ytm_amount']]

    return scope2_market_all


def s2_location_cal(df_elect, renw_location, coef):

    scope2_location = pd.merge(pd.merge(df_elect, renw_location, on=[
                               'site', 'period_start'], how='left'),  coef, on=['site'], how='left')
    scope2_location = scope2_location.fillna(0)
    if scope2_location.size != 0:

        scope2_location['amount'] = (
            scope2_location['amount_x'] - scope2_location['amount_y'])*scope2_location['coef'] / 1000
        scope2_location['ytm_amount'] = (
            scope2_location['ytm_amount_x'] - scope2_location['ytm_amount_y'])*scope2_location['coef'] / 1000
        scope2_location = scope2_location[[
            'period_start', 'amount', 'ytm_amount']]
        scope2_location_all = scope2_location.groupby(
            ['period_start']).sum().reset_index()
    else:

        scope2_location['amount'] = (
            scope2_location['amount_x'] - scope2_location['amount_y'])*scope2_location['coef'] / 1000
        scope2_location['ytm_amount'] = (
            scope2_location['ytm_amount_x'] - scope2_location['ytm_amount_y'])*scope2_location['coef'] / 1000
        scope2_location_all = scope2_location[[
            'period_start', 'amount', 'ytm_amount']]

    return scope2_location_all


def s1_s1n2_cal(scope2_location_all, scope2_market_all):

    scope1_all = scope2_location_all.copy()
    scope1_all['amount'] = scope1_all['amount']*0.06/(1-0.06)
    scope1_all['ytm_amount'] = scope1_all['ytm_amount']*0.06/(1-0.06)

    scope1n2 = scope1_all.append(scope2_market_all)
    if scope1n2.size != 0:

        scope1n2 = scope1n2.groupby(['period_start']).sum().reset_index()
    else:

        pass

    return scope1_all, scope1n2


def scope_cal_current_year(df_elect, df_solar, df_green_energy, df_green_elect, coef):

    if (df_solar.size == 0) and (df_green_energy.size == 0) and (df_green_elect.size == 0):

        renw_market = pd.DataFrame(
            columns=['site', 'amount', 'ytm_amount', 'period_start'])

    else:

        renw_market = df_solar.append(df_green_energy).append(df_green_elect)
        renw_market = renw_market.groupby(
            ['site', 'period_start']).sum().reset_index()

    if (df_solar.size == 0) and (df_green_elect.size == 0):

        renw_location = pd.DataFrame(
            columns=['site', 'amount', 'ytm_amount', 'period_start'])

    else:

        renw_location = df_solar.append(df_green_elect)
        renw_location = renw_location.groupby(
            ['site', 'period_start']).sum().reset_index()

    scope2_market_all = s2_market_cal(df_elect, renw_market, coef)

    scope2_location_all = s2_location_cal(df_elect, renw_location, coef)

    scope1_all, scope1n2 = s1_s1n2_cal(scope2_location_all, scope2_market_all)

    return scope2_market_all, scope2_location_all, scope1_all, scope1n2


def scope_cal_target_current(df_elect, df_solar, df_green_energy, df_green_elect, coef):

    if (df_solar.size == 0) and (df_green_energy.size == 0) and (df_green_elect.size == 0):

        renw_market = pd.DataFrame(
            columns=['site', 'amount', 'ytm_amount', 'period_start'])

    else:

        renw_market = df_solar.append(df_green_energy).append(df_green_elect)
        renw_market = renw_market.groupby(
            ['site', 'period_start']).sum().reset_index()

    if (df_solar.size == 0) and (df_green_elect.size == 0):

        renw_location = pd.DataFrame(
            columns=['site', 'amount', 'ytm_amount', 'period_start'])

    else:

        renw_location = df_solar.append(df_green_elect)
        renw_location = renw_location.groupby(
            ['site', 'period_start']).sum().reset_index()

    scope2_market_all = s2_market_cal(df_elect, renw_market, coef)

    scope2_location_all = s2_location_cal(df_elect, renw_location, coef)

    scope1_all, scope1n2 = s1_s1n2_cal(scope2_location_all, scope2_market_all)

    return scope2_market_all, scope2_location_all, scope1_all, scope1n2


def cal_scope_simulate(df1, df2, period_start):

    df_scope_simulate = df1.merge(df2, how='cross')

    if df_scope_simulate.size != 0:
        df_scope_simulate['period_start'] = period_start
        df_scope_simulate['amount'] = (
            (df_scope_simulate['amount_x']/df_scope_simulate['amount_y'])-1)*100
        df_scope_simulate['ytm_amount'] = (
            (df_scope_simulate['ytm_amount_x']/df_scope_simulate['ytm_amount_y'])-1)*100

    else:
        df_scope_simulate = df_scope_simulate[[
            'period_start_x', 'amount_x', 'ytm_amount_x']]
        df_scope_simulate.rename(columns={'period_start_x': 'period_start',
                                 'amount_x': 'amount', 'ytm_amount_x': 'ytm_amount'}, inplace=True)

    df_scope_simulate = df_scope_simulate[[
        'period_start', 'amount', 'ytm_amount']]

    return df_scope_simulate


def cal_scope_simulate_fix(df1, df2, period_start):

    df_scope_simulate = df1.merge(df2, on=['period_start'], how='left')

    if df_scope_simulate.size != 0:
        df_scope_simulate['period_start'] = period_start
        df_scope_simulate['amount'] = (
            (df_scope_simulate['amount_x']/df_scope_simulate['amount_y'])-1)*100
        df_scope_simulate['ytm_amount'] = (
            (df_scope_simulate['ytm_amount_x']/df_scope_simulate['ytm_amount_y'])-1)*100

    else:
        df_scope_simulate = df_scope_simulate[[
            'period_start_x', 'amount_x', 'ytm_amount_x']]
        df_scope_simulate.rename(columns={'period_start_x': 'period_start',
                                 'amount_x': 'amount', 'ytm_amount_x': 'ytm_amount'}, inplace=True)

    df_scope_simulate = df_scope_simulate[[
        'period_start', 'amount', 'ytm_amount']]

    return df_scope_simulate


def decarb_simulate_current_year(scope2_market_all, scope2_market_base_all, scope1n2, scope1n2_base_all, scope1n2_previous_all, period_start):

    scope2_simulate = cal_scope_simulate(
        scope2_market_all, scope2_market_base_all, period_start)

    scope1n2_simulate = cal_scope_simulate(
        scope1n2, scope1n2_base_all, period_start)

    scope1n2_simulate_sbti = cal_scope_simulate(
        scope1n2, scope1n2_previous_all, period_start)

    return scope2_simulate, scope1n2_simulate, scope1n2_simulate_sbti


def decarb_simulate_current_year_fix(scope2_market_all, scope2_market_base_all, scope1n2, scope1n2_base_all, scope1n2_previous_all, period_start):

    scope2_simulate = cal_scope_simulate_fix(
        scope2_market_all, scope2_market_base_all, period_start)

    scope1n2_simulate = cal_scope_simulate_fix(
        scope1n2, scope1n2_base_all, period_start)

    scope1n2_simulate_sbti = cal_scope_simulate_fix(
        scope1n2, scope1n2_previous_all, period_start)

    return scope2_simulate, scope1n2_simulate, scope1n2_simulate_sbti


def preprocess_sub_df(df, category_name):

    df_fix = df[['amount', 'ytm_amount', 'period_start']]
    df_fix = df_fix.groupby(['period_start']).sum().reset_index()
    df_fix['category'] = str(category_name)

    return df_fix


def cal_energy_saving(df, category):

    month_mapping = {'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06',
                     'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'}

    df_fix = pd.melt(df, id_vars=['year'],
                     var_name='month', value_name='amount')
    df_fix['month'].replace(month_mapping, inplace=True)

    df_fix['year'] = df_fix['year'].astype(str)
    df_fix['month'] = df_fix['month'].astype(str)

    df_fix['period_start'] = df_fix['year'] + '-' + df_fix['month'] + '-01'
    df_fix = df_fix[['period_start', 'amount']]
    df_fix = df_fix.groupby(['period_start']).sum().reset_index()
    df_fix['period_start'] = pd.to_datetime(
        df_fix['period_start'], errors='coerce')
    df_fix['year'] = df_fix['period_start'].apply(lambda x: x.year)
    df_fix = df_fix.sort_values(by=['year', 'period_start'])
    df_fix['amount'] = df_fix['amount'].astype(float)
    df_fix['ytm_amount'] = df_fix.groupby(['year'])['amount'].cumsum()
    df_fix = df_fix.drop('year', axis=1).reset_index(drop=True)
    df_fix['category'] = 'energy_efficiency'
    df_fix['type'] = category

    return df_fix


def staging_cal(table_name, stage):

    if table_name == 'green_elec_pre_contracts':

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
                month_end = dt.now().month - 1
                period_year_start = date(
                    dt.now().year, 1, 1).strftime("%Y-%m-%d")
                period_year_end = date(
                    dt.now().year, 12, 1).strftime("%Y-%m-%d")

            connect_string = engine.get_connect_string()
            db = create_engine(connect_string, echo=True)

            green_vol = pd.read_sql(
                f"""SELECT site, plant, provider_name, category1,  amount FROM staging.green_elect_vol where period_start >= '{period_year_start}' and period_start <= '{period_year_end}' and category1 = '轉供' and site !='ALL' and plant !='ALL'""", db)

            green_vol = green_vol[['provider_name', 'amount']]

            green_vol = green_vol.groupby(
                ['provider_name']).sum().reset_index()

            # area_mapping = pd.read_sql(f"""SELECT distinct provider_name, country, area FROM app.decarb_ww_meter_group""", db)

            area_mapping = pd.read_sql(
                f"""SELECT distinct provider_name, area FROM app.decarb_ww_meter_group""", db)

            green_vol = green_vol.merge(
                area_mapping, on='provider_name', how='left')

            green_vol['green_elect_type'] = '光電'

            # green_contract = pd.read_sql(f"""SELECT provider_name, contract_ytm_amount, ytm_amount, "year", last_update_time, country, area, green_elec_type, contract_price
            # FROM app.green_elec_pre_contracts  where year = '{year}' AND '光電' = ALL(green_elec_type)""" ,db)

            green_contract = pd.read_sql(f"""SELECT provider_name, contract_ytm_amount, "year", last_update_time, area, green_elec_type, contract_price
            FROM app.green_elec_pre_contracts  where year = '{year}' AND '光電' = ALL(green_elec_type)""", db)

            # green_vol_contract = green_contract.merge(green_vol, on=['provider_name','country','area'], how='left')

            green_vol_contract = green_contract.merge(
                green_vol, on=['provider_name', 'area'], how='left')

            green_vol_contract['ytm_amount'] = green_vol_contract['amount']

            # green_vol_contract = green_vol_contract[['provider_name', 'contract_ytm_amount', 'ytm_amount', 'year',  'last_update_time', 'country', 'area', 'green_elec_type',  'contract_price']]

            green_vol_contract = green_vol_contract[[
                'provider_name', 'contract_ytm_amount', 'year',  'last_update_time', 'area', 'green_elec_type',  'contract_price']]

            green_vol_contract['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            year = green_vol_contract['year']
            provider_name = green_vol_contract['provider_name']
            green_elec_type = green_vol_contract['green_elec_type']

            if len(year) == 0 or len(provider_name) == 0:

                pass

            elif green_vol_contract.size == 0:

                pass

            else:

                delete_query = f"""DELETE FROM app.green_elec_pre_contracts WHERE year IN {tuple(year)} AND provider_name IN {tuple(provider_name)} AND '光電' = ALL(green_elec_type)"""

                conn = db.connect()
                conn.execute(delete_query)

                green_vol_contract.to_sql('green_elec_pre_contracts', con=db,
                                          schema='app', if_exists='append', index=False, chunksize=1000)
                conn.close()

            return True

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] green elec pre contracts etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'decarb_elec_overview':

        connect_string = engine.get_connect_string()
        db = create_engine(connect_string, echo=True)

        try:

            for m in range(1, dt.now().month):

                # if m == 1:

                #     year = dt.now().year-1
                #     year_base = 2022
                #     year_previous = dt.now().year-2

                # #     month_start = 1
                # #     month_end = 12

                #     period_start = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")

                #     period_start_base = date(2022, 12, 1).strftime("%Y-%m-%d")

                #     period_start_previous = date(dt.now().year-2, 12, 1).strftime("%Y-%m-%d")

                # else:

                year = dt.now().year
                year_base = 2022
                year_previous = dt.now().year-1

            #     month_start = 1
            #     month_end = dt.now().month -3

                period_start = date(dt.now().year, m, 1).strftime("%Y-%m-%d")

                period_start_base = date(2022, m, 1).strftime("%Y-%m-%d")

                period_start_previous = date(
                    dt.now().year-1, m, 1).strftime("%Y-%m-%d")

                df_elect = pd.read_sql(
                    f"""SELECT  site, amount, ytm_amount , period_start FROM staging.electricity_decarb where period_start = '{period_start}' and bo = 'ALL' and site !='ALL'  """, db)

                df_elect_base = pd.read_sql(
                    f"""SELECT  site, amount, ytm_amount , period_start FROM staging.electricity_decarb where period_start = '{period_start_base}' and bo = 'ALL' and site !='ALL'  """, db)

                df_elect_previous = pd.read_sql(
                    f"""SELECT  site, amount, ytm_amount , period_start FROM staging.electricity_decarb where period_start = '{period_start_previous}' and bo = 'ALL' and site !='ALL' """, db)

                df_solar = pd.read_sql(
                    f"""SELECT site, amount, ytm_amount, period_start FROM staging.renewable_energy_decarb where category = 'solar_energy' and period_start ='{period_start}' and bo = 'ALL' and site !='ALL' """, db)

                df_solar_base = pd.read_sql(
                    f"""SELECT site, amount, ytm_amount, period_start FROM staging.renewable_energy_decarb where category = 'solar_energy' and period_start = '{period_start_base}' and bo = 'ALL' and site !='ALL' """, db)

                df_solar_previous = pd.read_sql(
                    f"""SELECT site, amount, ytm_amount, period_start FROM staging.renewable_energy_decarb where category = 'solar_energy' and period_start = '{period_start_previous}' and bo = 'ALL' and site !='ALL' """, db)

                df_green_energy = pd.read_sql(
                    f"""SELECT site, amount, ytm_amount, period_start FROM staging.renewable_energy_decarb where category = 'green_energy' and period_start ='{period_start}' and bo = 'ALL' and site !='ALL' """, db)

                df_green_energy_base = pd.read_sql(
                    f"""SELECT site, amount, ytm_amount, period_start FROM staging.renewable_energy_decarb where category = 'green_energy' and period_start = '{period_start_base}' and bo = 'ALL' and site !='ALL' """, db)

                df_green_energy_previous = pd.read_sql(
                    f"""SELECT site, amount, ytm_amount, period_start FROM staging.renewable_energy_decarb where category = 'green_energy' and period_start = '{period_start_previous}' and bo = 'ALL' and site !='ALL' """, db)

                df_green_elect = pd.read_sql(
                    f"""SELECT site, amount, ytm_amount, period_start FROM staging.renewable_energy_decarb where category = 'green_electricity' and period_start ='{period_start}' and bo = 'ALL' and site !='ALL' """, db)

                df_green_elect_base = pd.read_sql(
                    f"""SELECT site, amount, ytm_amount, period_start FROM staging.renewable_energy_decarb where category = 'green_electricity' and period_start ='{period_start_base}' and bo = 'ALL' and site !='ALL' """, db)

                df_green_elect_previous = pd.read_sql(
                    f"""SELECT site, amount, ytm_amount, period_start FROM staging.renewable_energy_decarb where category = 'green_electricity' and period_start ='{period_start_previous}' and bo = 'ALL' and site !='ALL' """, db)

                """
                coef_preprocess
                碳排係數依據廠區名稱 重新命名
                """

                site_dict = {'WCD-1': 'WCD', 'WMY-1': 'WMY', 'WMY-2': 'WMY', 'WVN-1': 'WVN', 'WIHK-1': 'WIHK', 'WIHK-2': 'WIHK',
                             'WZS-1': 'WZS', 'WZS-3': 'WZS', 'WZS-6': 'WZS', 'WZS-8': 'WZS', 'WKS-1': 'WKS', 'WKS-6': 'WKS', 'WKS-5': 'WKS'}

                coef = coef_preprocess(year, site_dict)

                coef_base = coef_preprocess(year_base, site_dict)

                coef_previous = coef_preprocess(year_previous, site_dict)

                """
                scope_cal_current_year 計算該年scope2_market_all,scope2_location_all,scope1_all,scope1n2

                Scope2 (location-based) = (總用電-太陽能-直購綠電) * 碳排係數 by site 後加總
                scope2_location_all

                Scope2 (market-based) = (總用電-太陽能-直購綠電-綠證) * 碳排係數 by site 後加總
                scope2_market_all

                Scope1 = Scope2(location-based)*6%/(1-6%)
                scope1_all

                Scope1+2 = Scope1 + Scope2(market-based)
                scope1n2
                """

                scope2_market_all, scope2_location_all, scope1_all, scope1n2 = scope_cal_current_year(
                    df_elect, df_solar, df_green_energy, df_green_elect, coef)

                # scope2_market_base_all,scope2_location_base_all,scope1_base_all,scope1n2_base_all = scope_cal_current_year(df_elect_base,df_solar_base,df_green_energy_base,df_green_elect_base,coef_base)

                scope2_market_base_all = pd.read_sql(
                    f"""SELECT amount, ytm_amount FROM app.decarb_elec_overview where year = 2022 and month = 12 and type = 'actual' and category = 'scope2_market'""", db)
                scope2_market_base_all['period_start'] = period_start

                scope2_location_base_all = pd.read_sql(
                    f"""SELECT amount, ytm_amount FROM app.decarb_elec_overview where year = 2022 and month = 12 and type = 'actual' and category = 'scope2_location'""", db)
                scope2_location_base_all['period_start'] = period_start

                scope1_base_all = pd.read_sql(
                    f"""SELECT amount, ytm_amount FROM app.decarb_elec_overview where year = 2022 and month = 12 and type = 'actual' and category = 'scope1'""", db)
                scope1_base_all['period_start'] = period_start

                scope1n2_base_all = scope1_base_all.append(
                    scope2_market_base_all)
                scope1n2_base_all = scope1n2_base_all.groupby(
                    ['period_start']).sum().reset_index()

                if year_previous == year_base:

                    scope2_market_previous_all = scope2_market_base_all.copy()
                    scope2_location_previous_all = scope2_location_base_all.copy()
                    scope1_previous_all = scope1_base_all.copy()
                    scope1n2_previous_all = scope1n2_base_all.copy()

                else:

                    scope2_market_previous_all, scope2_location_previous_all, scope1_previous_all, scope1n2_previous_all = scope_cal_current_year(
                        df_elect_previous, df_solar_previous, df_green_energy_previous, df_green_elect_previous, coef_previous)

                scope2_market_all['period_start'] = scope2_market_all['period_start'].astype(
                    str)
                scope2_market_base_all['period_start'] = scope2_market_base_all['period_start'].astype(
                    str)
                scope1n2['period_start'] = scope1n2['period_start'].astype(str)
                scope1n2_base_all['period_start'] = scope1n2_base_all['period_start'].astype(
                    str)
                scope1n2_previous_all['period_start'] = scope1n2_previous_all['period_start'].astype(
                    str)

                if m == dt.now().month-1:

                    scope2_simulate, scope1n2_simulate, scope1n2_simulate_sbti = decarb_simulate_current_year(
                        scope2_market_all, scope2_market_base_all, scope1n2, scope1n2_base_all, scope1n2_previous_all, period_start)

                else:

                    scope2_simulate, scope1n2_simulate, scope1n2_simulate_sbti = decarb_simulate_current_year_fix(
                        scope2_market_all, scope2_market_base_all, scope1n2, scope1n2_base_all, scope1n2_previous_all, period_start)

                """
                set category and type value
                實際type : actual
                總電量	: electricity
                自建太陽能 : solar
                直購綠電 : PPA
                購買綠證 : REC

                Scope 2 (Location-based) = scope2_location

                Scope 2 (Market-based) = scope2_market

                Scope 1 + 2 (Back-end 計算) = scope2_location*6%/(1-6%) + scope2_market

                scope2減碳模擬(base year) : scope2_decarb_simulate
                scope2_simulate = scope2(YOO) - scope2(基準年) / scope2(基準年)

                scope1+scope2減碳模擬(base year)  : scope1n2_decarb_simulate
                scope1n2_simulate = scope1&scope2(YOO) - scope1&scope2(基準年) / scope1&scope2(基準年)

                scope1+scope2減碳模擬(previous year) : scope1n2_decarb_simulate_sbti
                scope1n2_simulate_sbti = scope1&scope2(YOO) - scope1&scope2(YOO-1) / scope1&scope2(YOO-1)
                """

                df_elect_all = preprocess_sub_df(df_elect, 'electricity')
                df_solar_all = preprocess_sub_df(df_solar, 'solar')

                # df_elect_all_actual = df_elect_all[['period_start', 'amount', 'ytm_amount']]

                # df_solar_all_actual = df_solar_all[['period_start', 'amount', 'ytm_amount']]

                # df_elect_actual = df_elect_all_actual.append(df_solar_all_actual)

                # df_elect_actual_all = preprocess_sub_df(df_elect_actual, 'electricity')

                df_green_energy_all = preprocess_sub_df(df_green_energy, 'REC')
                df_green_elect_all = preprocess_sub_df(df_green_elect, 'PPA')

                scope2_market_all['category'] = 'scope2_market'
                scope2_location_all['category'] = 'scope2_location'
                scope1_all['category'] = 'scope1'
                scope2_simulate['category'] = 'scope2_decarb_simulate'
                scope1n2_simulate['category'] = 'scope1n2_decarb_simulate'
                scope1n2_simulate_sbti['category'] = 'scope1n2_decarb_simulate_sbti'

                df = df_elect_all.append(df_solar_all).append(df_green_energy_all).append(df_green_elect_all).append(scope2_market_all).append(
                    scope2_location_all).append(scope1_all).append(scope2_simulate).append(scope1n2_simulate).append(scope1n2_simulate_sbti)

                df['type'] = 'actual'
                df['year'] = pd.to_datetime(df['period_start']).dt.year
                df['month'] = pd.to_datetime(df['period_start']).dt.month

                df = df[['amount', 'ytm_amount',
                         'category', 'type', 'year', 'month']]

                year_ = df['year']
                month_ = df['month']
                type_ = df['type']
                category_ = df['category']

                if len(month_) == 0 or len(type_) == 0:

                    pass

                elif df.size == 0:

                    pass

                elif df.shape[0] == 1:

                    delete_query = f"""DELETE FROM app.decarb_elec_overview WHERE year  = {year_[0]} AND month = {month_[0]} AND type = '{type_[0]}' AND category = '{category_[0]}'"""

                    conn = db.connect()
                    conn.execute(delete_query)

                    df.to_sql('decarb_elec_overview', db, index=False,
                                     if_exists='append', schema='app', chunksize=10000)
                    conn.close()

                else:

                    delete_query = f"""DELETE FROM app.decarb_elec_overview WHERE year IN {tuple(year_)} AND month IN {tuple(month_)} AND type IN {tuple(type_)} AND category IN {tuple(category_)}"""

                    conn = db.connect()
                    conn.execute(delete_query)

                    df.to_sql('decarb_elec_overview', con=db, schema='app',
                              if_exists='append', index=False, chunksize=1000)
                    conn.close()

                total_ratio = pd.read_sql(
                    f"""SELECT sum(amount)/100 as "total_ratio" FROM staging.renewable_setting where year = {year} and category in ('REC','PPA','solar')""", db)

                elect_target = pd.read_sql(
                    f"""SELECT plant as site, amount, ytm_amount, period_start FROM staging.elect_total where period_start ='{period_start}' and category = 'target' """, db)

                solar_target = pd.read_sql(
                    f"""SELECT plant as site , amount, ytm_amount, period_start FROM app.solar_energy_overview  where category = 'target' and period_start ='{period_start}' and site !='ALL' and plant !='ALL' """, db)

                grelect_target = pd.read_sql(
                    f"""SELECT "year", "month", site, amount FROM app.provider_plant_list where year = {year} and month >= 1 and month <= {m} """, db)

                grelect_target['year'] = grelect_target['year'].astype(str)
                grelect_target['month'] = grelect_target['month'].astype(str)
                grelect_target['period_start'] = grelect_target['year'] + \
                    '-' + grelect_target['month'] + '-01'
                grelect_target['period_start'] = pd.to_datetime(
                    grelect_target['year'] + '-' + grelect_target['month'] + '-01')
                grelect_target = grelect_target[[
                    'year', 'site', 'amount', 'period_start']]
                grelect_target = grelect_target.sort_values(
                    by=['year', 'site', 'period_start'])
                grelect_target['amount'] = grelect_target['amount'].astype(
                    float)
                grelect_target['ytm_amount'] = grelect_target.groupby(['year', 'site'])[
                    'amount'].cumsum()
                grelect_target = grelect_target.drop(
                    'year', axis=1).reset_index(drop=True)
                grelect_target = grelect_target[grelect_target['period_start']
                                                == period_start]

                elect_target['period_start'] = elect_target['period_start'].astype(
                    str)
                solar_target['period_start'] = solar_target['period_start'].astype(
                    str)
                grelect_target['period_start'] = grelect_target['period_start'].astype(
                    str)

                elect_target_totall = elect_target.groupby(
                    ['period_start']).sum().reset_index()
                solar_target_totall = solar_target.groupby(
                    ['period_start']).sum().reset_index()
                grelect_target_totall = grelect_target.groupby(
                    ['period_start']).sum().reset_index()
                

                grenergy_target_toatal_pre = pd.merge(pd.merge(pd.merge(elect_target_totall, solar_target_totall, on=[
                                                      'period_start'], how='left'),  grelect_target_totall, on=['period_start'], how='left'), total_ratio, how='cross')
                grenergy_target_toatal_pre = grenergy_target_toatal_pre.fillna(
                    0)

                if grenergy_target_toatal_pre.size != 0:
                    grenergy_target_toatal_pre['amount_grenergy'] = grenergy_target_toatal_pre['amount_x'] * \
                        grenergy_target_toatal_pre['total_ratio'] - \
                        grenergy_target_toatal_pre['amount_y'] - \
                        grenergy_target_toatal_pre['amount']
                    grenergy_target_toatal_pre['ytm_amount_grenergy'] = grenergy_target_toatal_pre['ytm_amount_x'] * \
                        grenergy_target_toatal_pre['total_ratio'] - \
                        grenergy_target_toatal_pre['ytm_amount_y'] - \
                        grenergy_target_toatal_pre['ytm_amount']
                    grenergy_target_toatal = grenergy_target_toatal_pre[[
                        'period_start', 'amount_grenergy', 'ytm_amount_grenergy']]
                    grenergy_target_toatal.rename(columns={
                                                  'amount_grenergy': 'amount', 'ytm_amount_grenergy': 'ytm_amount'}, inplace=True)

                else:
                    grenergy_target_toatal = grenergy_target_toatal_pre

                grenergy_target_pre = pd.merge(pd.merge(pd.merge(elect_target, solar_target, on=[
                                               'site', 'period_start'], how='left'),  grelect_target, on=['site', 'period_start'], how='left'), total_ratio, how='cross')
                grenergy_target_pre = grenergy_target_pre.fillna(0)

                if grenergy_target_pre.size != 0:
                    grenergy_target_pre['amount_grenergy'] = grenergy_target_pre['amount_x'] * \
                        grenergy_target_pre['total_ratio'] - \
                        grenergy_target_pre['amount_y'] - \
                        grenergy_target_pre['amount']
                    grenergy_target_pre['ytm_amount_grenergy'] = grenergy_target_pre['ytm_amount_x'] * \
                        grenergy_target_pre['total_ratio'] - \
                        grenergy_target_pre['ytm_amount_y'] - \
                        grenergy_target_pre['ytm_amount']
                    grenergy_target = grenergy_target_pre[[
                        'site', 'period_start', 'amount_grenergy', 'ytm_amount_grenergy']]
                    grenergy_target.rename(columns={
                                           'amount_grenergy': 'amount', 'ytm_amount_grenergy': 'ytm_amount'}, inplace=True)

                else:
                    grenergy_target = grenergy_target_pre

                """
                scope_cal_target_current 計算該年的目標值 scope2_market_target_all,scope2_location_target_all,scope1_target_all,scope1n2_target

                """

                scope2_market_target_all, scope2_location_target_all, scope1_target_all, scope1n2_target = scope_cal_target_current(
                    elect_target, solar_target, grenergy_target, grelect_target, coef)

                # scope2_market_base_all,scope2_location_base_all,scope1_base_all,scope1n2_base_all = scope_cal_target_current(elect_target,solar_target,coef_base)

                # scope2_market_previous_all,scope2_location_previous_all,scope1_previous_all,scope1n2_previous_all = scope_cal_target_current(df_elect_previous,df_solar_previous,coef_previous)
                scope2_market_target_all['period_start'] = scope2_market_target_all['period_start'].astype(
                    str)
                scope2_market_base_all['period_start'] = scope2_market_base_all['period_start'].astype(
                    str)
                scope1n2_target['period_start'] = scope1n2_target['period_start'].astype(
                    str)
                scope1n2_base_all['period_start'] = scope1n2_base_all['period_start'].astype(
                    str)
                scope1n2_previous_all['period_start'] = scope1n2_previous_all['period_start'].astype(
                    str)

                scope2_simulate_target, scope1n2_simulate_target, scope1n2_simu_target_sbti = decarb_simulate_current_year_fix(
                    scope2_market_target_all, scope2_market_base_all, scope1n2_target, scope1n2_base_all, scope1n2_previous_all, period_start)

                """
                set category and type value
                目標type : target
                總電量	: electricity
                自建太陽能 : solar

                Scope 2 (Location-based) = scope2_market_target_all

                Scope 2 (Market-based) = scope2_location_target_all

                Scope 1 + 2 (Back-end 計算) = scope2_location_target_all*6%/(1-6%) + scope2_market_target_all

                scope2減碳模擬(base year) : scope2_decarb_simulate
                scope2_simulate_target = scope2(YOO) - scope2(基準年) / scope2(基準年)

                scope1+scope2減碳模擬(base year)  : scope1n2_decarb_simulate
                scope1n2_simulate_target = scope1&scope2(YOO) - scope1&scope2(基準年) / scope1&scope2(基準年)

                scope1+scope2減碳模擬(previous year) : scope1n2_decarb_simulate_sbti
                scope1n2_simu_target_sbti = scope1&scope2(YOO) - scope1&scope2(YOO-1) / scope1&scope2(YOO-1)
                """

                elect_target_all = preprocess_sub_df(
                    elect_target, 'electricity')
                solar_target_all = preprocess_sub_df(solar_target, 'solar')
                green_energy_target_all = grenergy_target_toatal
                green_energy_target_all['category'] = 'REC'
                # green_elect_target_all = preprocess_sub_df(
                #     grelect_target, 'PPA')
                
                #set PPA YTM target into PPA year target 
                PPA_ratio = pd.read_sql(f"""SELECT amount/100 as "ratio" FROM staging.renewable_setting where year = {year} and category in ('PPA')""", db)
                green_elect_target_all = elect_target_all.copy()
                green_elect_target_all['amount'] = green_elect_target_all['amount'] * PPA_ratio['ratio'][0]
                green_elect_target_all['ytm_amount'] = green_elect_target_all['ytm_amount'] * PPA_ratio['ratio'][0]
                green_elect_target_all['category'] = 'PPA'

                scope2_market_target_all['category'] = 'scope2_market'
                scope2_location_target_all['category'] = 'scope2_location'
                scope1_target_all['category'] = 'scope1'
                scope2_simulate_target['category'] = 'scope2_decarb_simulate'
                scope1n2_simulate_target['category'] = 'scope1n2_decarb_simulate'
                scope1n2_simu_target_sbti['category'] = 'scope1n2_decarb_simulate_sbti'

                """因應綠證為每季資料 針對3,6,9,12月 amount 進行處理 其餘月份amount為0"""

                if pd.to_datetime(green_energy_target_all['period_start']).dt.month[0] == 3:

                    green_energy_target_all['amount'] = green_energy_target_all['ytm_amount']

                    green_energy_target_all_fix = green_energy_target_all

                elif pd.to_datetime(green_energy_target_all['period_start']).dt.month[0] in ( 4,5) :
                    green_energy_target_filter = pd.read_sql(
                        f"""select ytm_amount from app.decarb_elec_overview WHERE year = {year} AND month = 3 AND type ='target' AND category ='REC'""", db)

                    green_energy_target_all['amount'] = 0
                    green_energy_target_all['ytm_amount'] = green_energy_target_filter['ytm_amount']

                    green_energy_target_all_fix = green_energy_target_all


                elif pd.to_datetime(green_energy_target_all['period_start']).dt.month[0] == 6:

                    green_energy_target_filter = pd.read_sql(
                        f"""select amount from app.decarb_elec_overview WHERE year = {year} AND month = 3 AND type ='target' AND category ='REC'""", db)

                    if green_energy_target_all.size != 0:

                        green_energy_target_all_fix = green_energy_target_adjust(
                            green_energy_target_all, green_energy_target_filter)

                elif pd.to_datetime(green_energy_target_all['period_start']).dt.month[0] in (7,8) :
                    green_energy_target_filter = pd.read_sql(
                        f"""select ytm_amount from app.decarb_elec_overview WHERE year = {year} AND month = 6 AND type ='target' AND category ='REC'""", db)

                    green_energy_target_all['amount'] = 0
                    green_energy_target_all['ytm_amount'] = green_energy_target_filter['ytm_amount']

                    green_energy_target_all_fix = green_energy_target_all

                elif pd.to_datetime(green_energy_target_all['period_start']).dt.month[0] == 9:

                    green_energy_target_filter = pd.read_sql(
                        f"""select sum(amount) as amount from app.decarb_elec_overview WHERE year = {year} AND month in (3,6) AND type ='target' AND category ='REC'""", db)

                    if green_energy_target_all.size != 0:
                        green_energy_target_all_fix = green_energy_target_adjust(
                            green_energy_target_all, green_energy_target_filter)

                elif pd.to_datetime(green_energy_target_all['period_start']).dt.month[0] in (10,11) :
                    green_energy_target_filter = pd.read_sql(
                        f"""select ytm_amount from app.decarb_elec_overview WHERE year = {year} AND month = 9 AND type ='target' AND category ='REC'""", db)

                    green_energy_target_all['amount'] = 0
                    green_energy_target_all['ytm_amount'] = green_energy_target_filter['ytm_amount']

                    green_energy_target_all_fix = green_energy_target_all

                elif pd.to_datetime(green_energy_target_all['period_start']).dt.month[0] == 12:

                    green_energy_target_filter = pd.read_sql(
                        f"""select sum(amount) as amount from app.decarb_elec_overview WHERE year = {year} AND month in (3,6,9) AND type ='target' AND category ='REC'""", db)

                    if green_energy_target_all.size != 0:
                        green_energy_target_all_fix = green_energy_target_adjust(
                            green_energy_target_all, green_energy_target_filter)

                elif pd.to_datetime(green_energy_target_all['period_start']).dt.month[0] == 1 or pd.to_datetime(green_energy_target_all['period_start']).dt.month[0] == 2:

                    green_energy_target_all['ytm_amount'] = 0
                    green_energy_target_all['amount'] = 0

                    green_energy_target_all_fix = green_energy_target_all

                else:
                    # green_energy_target_all['ytm_amount'] = 0

                    green_energy_target_all['amount'] = 0
                    green_energy_target_all_fix = green_energy_target_all

                green_energy_target_all_fix = green_energy_target_all_fix[[
                                    'period_start', 'amount', 'ytm_amount']]

                green_energy_target_all_fix['category'] = 'REC'

                # green_energy_target_all = green_energy_target_all[[
                #     'period_start', 'amount', 'ytm_amount']]

                df_target = elect_target_all.append(solar_target_all).append(green_energy_target_all_fix).append(green_elect_target_all).append(scope2_market_target_all).append(
                    scope2_location_target_all).append(scope1_target_all).append(scope2_simulate_target).append(scope1n2_simulate_target).append(scope1n2_simu_target_sbti)

                df_target['type'] = 'target'
                df_target['year'] = pd.to_datetime(
                    df_target['period_start']).dt.year
                df_target['month'] = pd.to_datetime(
                    df_target['period_start']).dt.month

                df_target = df_target[[
                    'amount', 'ytm_amount', 'category', 'type', 'year', 'month']]

                year_target = df_target['year']
                month_target = df_target['month']
                type_target = df_target['type']
                category_target = df_target['category']

                if len(month_target) == 0 or len(type_target) == 0:

                    pass

                elif df_target.size == 0:

                    pass

                else:

                    delete_query = f"""DELETE FROM app.decarb_elec_overview WHERE year IN {tuple(year_target)} AND month IN {tuple(month_target)} AND type IN {tuple(type_target)} AND category IN {tuple(category_target)}"""

                    conn = db.connect()
                    conn.execute(delete_query)

                    df_target.to_sql('decarb_elec_overview', con=db, schema='app',
                                     if_exists='append', index=False, chunksize=1000)
                    conn.close()

            return True

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] decarb elect overview etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'energy_efficiency':

        connect_string = engine.get_connect_string()
        db = create_engine(connect_string, echo=True)

        try:

            df_actual = pd.read_sql(
                f"""SELECT "year", jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, "dec" FROM raw.energy_saving where category = 'actual'""", db)

            df_target = pd.read_sql(
                f"""SELECT "year", jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, "dec" FROM raw.energy_saving where category = 'target'""", db)

            saving_actual = cal_energy_saving(df_actual, 'actual')

            saving_target = cal_energy_saving(df_target, 'target')

            energy_saving = saving_actual.append(
                saving_target).reset_index(drop=True)

            energy_saving['year'] = pd.to_datetime(
                energy_saving['period_start']).dt.year
            energy_saving['month'] = pd.to_datetime(
                energy_saving['period_start']).dt.month

            energy_saving = energy_saving[[
                'amount', 'ytm_amount', 'category', 'type', 'year', 'month']]

            year_value = energy_saving['year']
            month_value = energy_saving['month']
            type_value = energy_saving['type']
            category_value = energy_saving['category']

            if len(month_value) == 0 or len(type_value) == 0:

                pass

            elif energy_saving.size == 0:

                pass

            else:

                delete_query = f"""DELETE FROM app.decarb_elec_overview WHERE year IN {tuple(year_value)} AND month IN {tuple(month_value)} AND type IN {tuple(type_value)} AND category IN {tuple(category_value)}"""

                conn = db.connect()
                conn.execute(delete_query)

                energy_saving.to_sql('decarb_elec_overview', con=db, schema='app',
                                     if_exists='append', index=False, chunksize=1000)
                conn.close()

            return True

        except Exception as e:

            error = str(e)
            mail = MailService(
                '[failed][{}] energy_efficiency etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error
