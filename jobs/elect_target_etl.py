import calendar
from datetime import date
from datetime import datetime as dt
from datetime import timedelta

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import *

from models import engine
from services.mail_service import MailService


# scope_cal 計算該年scope1和scope2
def scope_cal(year):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    site_dict = {'WIHK1': 'WIHK-1', 'WIHK2': 'WIHK-2',
                 'WMYP1': 'WMY', 'WMYP2': 'WMY'}

    coef = pd.read_sql(
        f"""SELECT "year", site, amount as coef FROM staging.decarb_carbon_coef where year = {year}""", db)
    coef['site'] = coef['site'].replace(site_dict)
    coef.drop_duplicates(inplace=True)

    target_simulate = pd.read_sql(
        f"""SELECT site, "year", amount FROM app.decarb_elect_simulate where "version" = (SELECT MAX("version") FROM app.decarb_elect_simulate) and "version_year" = (SELECT MAX("version_year") FROM app.decarb_elect_simulate) and year = {year}""", db)

    s2_loaction_ratio = pd.read_sql(f"""SELECT  "year", (1 - sum(amount)/100) as ratio FROM staging.renewable_setting where category in ('PPA','solar') and year = {year}
    group by year;""", db)
    s2_loaction_ratio['category'] = 'location'

    s2_market_ratio = pd.read_sql(f"""SELECT  "year", (1 - sum(amount)/100) as ratio FROM staging.renewable_setting where category in ('PPA','solar','REC') and year = {year}
    group by year;""", db)
    s2_market_ratio['category'] = 'market'

    s2 = s2_loaction_ratio.append(s2_market_ratio)

    target_s2 = target_simulate.merge(s2, on='year', how='left')

    target_s2_coef = target_s2.merge(coef, on=['year', 'site'], how='left')

    target_s2_coef.fillna(0, inplace=True)

    target_s2_coef['amount'] = target_s2_coef['amount'] * \
        target_s2_coef['ratio']*target_s2_coef['coef']/1000

    s2_location = target_s2_coef.loc[target_s2_coef['category'] == 'location', [
        'year', 'amount']]

    s2_location_all = s2_location.groupby(['year']).sum().reset_index()

    s2_market = target_s2_coef.loc[target_s2_coef['category'] == 'market', [
        'year', 'amount']]

    s2_market_all = s2_market.groupby(['year']).sum().reset_index()

    scope1_all = s2_location_all.copy()

    scope1_all['amount'] = scope1_all['amount']*0.06/(1-0.06)

    scope1n2 = scope1_all.append(s2_market_all)

    scope1n2_all = scope1n2.groupby(['year']).sum().reset_index()

    return s2_location_all, s2_market_all, scope1_all, scope1n2_all


def base_scope_cal_elec_overview(year):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    if year == 2022:
        scope1_base_all = pd.read_sql(
            f"""SELECT "year",  ytm_amount as amount FROM app.decarb_elec_overview where year = {year} and month = 12 and category = 'scope1' and type = 'actual'""", db)
        s2_location_base_all = pd.read_sql(
            f"""SELECT "year",  ytm_amount as amount FROM app.decarb_elec_overview where year = {year} and month = 12 and category = 'scope2_location' and type = 'actual'""", db)
        s2_market_base_all = pd.read_sql(
            f"""SELECT "year",  ytm_amount as amount FROM app.decarb_elec_overview where year = {year} and month = 12 and category = 'scope2_market' and type = 'actual'""", db)

        scope1n2_base = scope1_base_all.append(s2_market_base_all)
        scope1n2_base_all = scope1n2_base.groupby(['year']).sum().reset_index()

    else:
        scope1_base_all = pd.read_sql(
            f"""SELECT "year",  amount as amount FROM staging.renewable_setting where year = {year} and category = 'scope1' """, db)
        s2_location_base_all = pd.read_sql(
            f"""SELECT "year",  amount as amount FROM staging.renewable_setting where year = {year} and category = 'scope2_location' """, db)
        s2_market_base_all = pd.read_sql(
            f"""SELECT "year",  amount as amount FROM staging.renewable_setting where year = {year} and category = 'scope2_market' """, db)

        scope1n2_base = scope1_base_all.append(s2_market_base_all)
        scope1n2_base_all = scope1n2_base.groupby(['year']).sum().reset_index()

    return s2_location_base_all, s2_market_base_all, scope1_base_all, scope1n2_base_all


def base_scope_cal_stage_table(year):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    period_start_base = '2022-12-01'
    year = 2022

    df_elect_base = pd.read_sql(
        f"""SELECT  site, ytm_amount as amount , period_start FROM staging.electricity_decarb where period_start = '{period_start_base}' and bo = 'ALL' and site !='ALL' """, db)
    df_solar_base = pd.read_sql(
        f"""SELECT site, ytm_amount as amount, period_start FROM staging.renewable_energy_decarb where category = 'solar_energy' and period_start = '{period_start_base}' and bo = 'ALL' and site !='ALL' """, db)
    df_green_energy_base = pd.read_sql(
        f"""SELECT site, ytm_amount as amount, period_start FROM staging.renewable_energy_decarb where category = 'green_energy' and period_start = '{period_start_base}' and bo = 'ALL' and site !='ALL' """, db)
    df_green_elect_base = pd.read_sql(
        f"""SELECT site, ytm_amount as amount, period_start FROM staging.renewable_energy_decarb where category = 'green_electricity' and period_start ='{period_start_base}' and bo = 'ALL' and site !='ALL' """, db)

    site_dict = {'WIHK1': 'WIHK-1', 'WIHK2': 'WIHK-2',
                 'WMYP1': 'WMY', 'WMYP2': 'WMY'}
    coef_base = pd.read_sql(
        f"""SELECT site , amount as coef FROM staging.cfg_carbon_coef where "year" = {year} """, db)
    coef_base['site'] = coef_base['site'].replace(site_dict)
    coef_base.drop_duplicates(inplace=True)

    renw_market_base = df_solar_base.append(
        df_green_energy_base).append(df_green_elect_base)
    renw_market_base = renw_market_base.groupby(
        ['site', 'period_start']).sum().reset_index()

    renw_location_base = df_solar_base.append(df_green_elect_base)
    renw_location_base = renw_location_base.groupby(
        ['site', 'period_start']).sum().reset_index()

    scope2_market_base = pd.merge(pd.merge(df_elect_base, renw_market_base, on=[
                                  'site', 'period_start'], how='left'),  coef_base, on=['site'], how='left')
    scope2_market_base = scope2_market_base.fillna(0)
    scope2_market_base['amount'] = (scope2_market_base['amount_x'] -
                                    scope2_market_base['amount_y'])*scope2_market_base['coef'] / 1000
    # scope2_market_base['ytm_amount'] = (scope2_market_base['ytm_amount_x'] - scope2_market_base['ytm_amount_y'])*scope2_market_base['coef'] /1000
    scope2_market_base = scope2_market_base[['period_start', 'amount']]
    scope2_market_base_all = scope2_market_base.groupby(
        ['period_start']).sum().reset_index()

    scope2_location_base = pd.merge(pd.merge(df_elect_base, renw_location_base, on=[
                                    'site', 'period_start'], how='left'),  coef_base, on=['site'], how='left')
    scope2_location_base = scope2_location_base.fillna(0)
    scope2_location_base['amount'] = (scope2_location_base['amount_x'] -
                                      scope2_location_base['amount_y'])*scope2_location_base['coef'] / 1000
    # scope2_location_base['ytm_amount'] = (scope2_location_base['ytm_amount_x'] - scope2_location_base['ytm_amount_y'])*scope2_location_base['coef'] /1000
    scope2_location_base = scope2_location_base[['period_start', 'amount']]
    scope2_location_base_all = scope2_location_base.groupby(
        ['period_start']).sum().reset_index()

    scope1_base_all = scope2_location_base_all.copy()
    scope1_base_all['amount'] = scope1_base_all['amount']*0.06/(1-0.06)
    # scope1_base_all['ytm_amount'] = scope1_base_all['ytm_amount']*0.06/(1-0.06)

    scope1n2_base = scope1_base_all.append(scope2_market_base_all)
    scope1n2_base_all = scope1n2_base.groupby(
        ['period_start']).sum().reset_index()

    return scope2_location_base_all, scope2_market_base_all, scope1_base_all, scope1n2_base_all


def decarb_simulate(df_yoo, df_before, category_name):

    df = df_yoo.merge(df_before, how='cross')
    df['amount'] = ((df['amount_x']/df['amount_y'])-1)*100
    if 'year_x' in df.columns.tolist():
        df.rename(columns={'year_x': 'year'}, inplace=True)
    df = df[['year', 'amount']]
    df['category'] = str(category_name)

    return df


def decarb_renew_setting_etl(stage):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    try:

        for year in range(dt.now().year, dt.now().year+8):

            if dt.now().month == 1:

                year = dt.now().year-1
                base_year = 2022
                previous_year = dt.now().year-2

            else:

                year = year
                base_year = 2022
                previous_year = year-1

                """
                scope_cal 計算該年scope1和scope2
                """
                s2_location_all, s2_market_all, scope1_all, scope1n2_all = scope_cal(
                    year)

                """
                base_scope_cal_elec_overview   用app.decarb_elec_overview計算基準年 scope1和scope2
                """
                # s2_location_base,s2_market_base,scope1_base,scope1n2_base = base_scope_cal_stage_table(base_year)
                s2_location_base, s2_market_base, scope1_base, scope1n2_base = base_scope_cal_elec_overview(
                    base_year)

                """
                判斷是否基準年 為 前一年, 2023年和未來年份 分開判斷, 依據之後資料來源做調整
                """

                if previous_year == 2022:
                    # 同基準年2022
                    # s2_location_previous,s2_market_previous,scope1_previous,scope1n2_previous = base_scope_cal_stage_table(previous_year)
                    s2_location_previous, s2_market_previous, scope1_previous, scope1n2_previous = base_scope_cal_elec_overview(
                        previous_year)

                else:

                    s2_location_previous, s2_market_previous, scope1_previous, scope1n2_previous = base_scope_cal_elec_overview(
                        previous_year)

                """
                計算減碳模擬

                基準年 : Y22

                scope2減碳模擬(base year)
                scope2_decarb_simulate = scope2(YOO) - scope2(基準年) / scope2(基準年)

                scope1+scope2減碳模擬(base year)
                scope1n2_decarb_simulate = scope1&scope2(YOO) - scope1&scope2(基準年) / scope1&scope2(基準年)

                scope1+scope2減碳模擬(previous year)
                scope1n2_decarb_simulate_sbti = scope1&scope2(YOO) - scope1&scope2(YOO-1) / scope1&scope2(YOO-1)
                """

                scope2_decarb_simulate = decarb_simulate(
                    s2_market_all, s2_market_base, 'scope2_decarb_simulate')
                scope1n2_decarb_simulate = decarb_simulate(
                    scope1n2_all, scope1n2_base, 'scope1n2_decarb_simulate')
                scope1n2_decarb_simulate_sbti = decarb_simulate(
                    scope1n2_all, scope1n2_previous, 'scope1n2_decarb_simulate_sbti')

                """
                set category value

                Scope 2 (Location-based) = scope2_location

                Scope 2 (Market-based) = scope2_market

                Scope 1 + 2 (Back-end計算) = scope2_location*6%/(1-6%) + scope2_market
                """

                s2_location_all['category'] = 'scope2_location'
                s2_market_all['category'] = 'scope2_market'
                scope1_all['category'] = 'scope1'

                df_renew_setting = s2_location_all.append(s2_market_all).append(scope1_all).append(
                    scope2_decarb_simulate).append(scope1n2_decarb_simulate).append(scope1n2_decarb_simulate_sbti)

                year = df_renew_setting['year']
                category = df_renew_setting['category']

                if len(year) == 0 or len(category) == 0:

                    pass

                elif df_renew_setting.size == 0:

                    pass

                else:

                    delete_query = f"""DELETE FROM staging.renewable_setting WHERE year IN {tuple(year)} AND category IN {tuple(category)}"""

                    conn = db.connect()
                    conn.execute(delete_query)

                    df_renew_setting.to_sql(
                        'renewable_setting', con=db, schema='staging', if_exists='append', index=False, chunksize=1000)
                    conn.close()

        return True

    except Exception as e:
        error = str(e)
        mail = MailService(
            '[failed][{}] renewable setting api info cron job report'.format(stage))
        mail.send_text('failed: {}'.format(error))
        return error
