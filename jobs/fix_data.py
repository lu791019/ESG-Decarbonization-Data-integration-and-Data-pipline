from datetime import date
from datetime import datetime as dt

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import *

from models import engine

connect_eco_string = engine.get_connect_string()

db_eco = create_engine(connect_eco_string, echo=True)

def useful_datetime(i):

    period_start = (date(dt.now().year, dt.now().month, 1) -
                    relativedelta(months=i)).strftime("%Y-%m-%d")

    return period_start


def update_to_raw(shcema, df_target, raw_table, period_start, site):

    conn = db_eco.connect()
    conn.execute(
        f"""Delete From {shcema}.{raw_table} where period_start = '{period_start}' AND site = '{site}'""")
    df_target.to_sql(str(raw_table), conn, index=False,
                     if_exists='append', schema=shcema, chunksize=10000)
    conn.close()

def fix_raw(current_months, raw_table,category):

    period_start_new = useful_datetime(current_months)

    period_start_old = useful_datetime(6)

    df_new = pd.read_sql(f"""SELECT * FROM raw.{raw_table} where  period_start = '{period_start_new}' and category2 = '{category}'""", con=db_eco)

    if df_new.empty:
        new_row = {'site': '', 'amount': 0, 'period_start': '','category1': '綠色能源','category2': str(category)}
        df_new.loc[0] = new_row

    df_new['type'] = ''

    if 'last_update_time' in df_new.columns:

        df_new.drop('last_update_time', axis=1, inplace=True)

    df_old = pd.read_sql(f"""SELECT * FROM raw.{raw_table} where  period_start = '{period_start_old}' and category2 = '{category}'""", con=db_eco)

    df_old['type'] = ''

    if 'last_update_time' in df_old.columns:

        df_old.drop('last_update_time', axis=1, inplace=True)

    df_fix = df_new.copy()

    if raw_table == 'renewable_energy_decarb':

        df_fix.drop(columns = ['id'], inplace=True)


    for i in set(df_old['site']).difference(df_new['site']):

        df_fix['site'] = str(i)
        df_fix['amount'] = 0
        df_fix['period_start'] = period_start_new
        df_fix.drop_duplicates(inplace=True)


        if df_fix.size !=0:

            period_start_value = df_fix['period_start']
            site_value = df_fix['site']
            category2_value = df_fix['category2']

            if len(period_start_value) == 0 or len(site_value) == 0:

                pass

            elif df_fix.size ==0:

                pass

            elif df_fix.shape[0] == 1:

                delete_query = f"""DELETE FROM raw.{raw_table} WHERE site = '{site_value[0]}' AND category2 = '{category2_value[0]}' AND period_start = '{period_start_value[0]}'"""

                conn = db_eco.connect()
                conn.execute(delete_query)

                df_fix.to_sql(str(raw_table), db_eco, index=False,
                                if_exists='append', schema='raw', chunksize=10000)
                conn.close()

            else:

                delete_query = f"""DELETE FROM raw.{raw_table} WHERE site IN {tuple(site_value)} AND category2 IN {tuple(category2_value)} AND period_start IN {tuple(str(date) for date in period_start_value)}"""

                conn = db_eco.connect()
                conn.execute(delete_query)

                df_fix.to_sql(str(raw_table), con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)
                conn.close()

def fix_raw_elect_decarb(current_months, raw_table):

    period_start_new = useful_datetime(current_months)

    period_start_old = useful_datetime(6)

    df_new = pd.read_sql(f"""SELECT * FROM raw.{raw_table} where  period_start = '{period_start_new}'""", con=db_eco)

    if df_new.empty:
        new_row = {'site': '', 'amount': 0, 'period_start': '','unit':'度'}
        df_new.loc[0] = new_row

    df_new['type'] = ''

    if 'last_update_time' in df_new.columns:

        df_new.drop('last_update_time', axis=1, inplace=True)

    df_old = pd.read_sql(f"""SELECT * FROM raw.{raw_table} where  period_start = '{period_start_old}'""", con=db_eco)

    df_old['type'] = ''

    if 'last_update_time' in df_old.columns:

        df_old.drop('last_update_time', axis=1, inplace=True)

    df_fix = df_new.copy()


    for i in set(df_old['site']).difference(df_new['site']):


        df_fix['site'] = str(i)
        df_fix['amount'] = 0
        df_fix['period_start'] = period_start_new
        df_fix.drop_duplicates(inplace=True)


        if df_fix.size !=0:

            period_start_value = df_fix['period_start']
            site_value = df_fix['site']

            if len(period_start_value) == 0 or len(site_value) == 0:

                pass

            elif df_fix.size ==0:

                pass

            elif df_fix.shape[0] == 1:

                delete_query = f"""DELETE FROM raw.{raw_table} WHERE site = '{site_value[0]}' AND period_start = '{period_start_value[0]}'"""

                conn = db_eco.connect()
                conn.execute(delete_query)

                df_fix.to_sql(str(raw_table), db_eco, index=False,
                                if_exists='append', schema='raw', chunksize=10000)
                conn.close()

            else:

                delete_query = f"""DELETE FROM raw.{raw_table} WHERE site IN {tuple(site_value)} AND period_start IN {tuple(str(date) for date in period_start_value)}"""

                conn = db_eco.connect()
                conn.execute(delete_query)

                df_fix.to_sql(str(raw_table), con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)
                conn.close()


def source_status_old(source_table,target_table):

    if source_table =='electricity_total_decarb':

        df = pd.read_sql(f"""select distinct site ,period_start,type from raw.{source_table} where type !=''""", con=db_eco)

        df['item'] = '實際用電'

    if source_table =='renewable_energy_decarb':

        item_dict = {'光伏':'自建太陽能','綠電':'直購綠電','綠證':'購買綠證'}

        df = pd.read_sql(f"""select distinct site ,category2 as "item", period_start,type from raw.{source_table} where type !=''""", con=db_eco)

        df['item'] = df['item'].replace(item_dict)

    site_mapping = pd.read_sql(f"""SELECT DISTINCT site_category, site FROM raw.plant_mapping """, con=db_eco)

    df_target = df.merge(site_mapping, on='site', how='left')

    df_target.dropna(inplace=True)

    df_target['year'] = pd.to_datetime(df_target['period_start']).dt.year

    df_target['month'] = pd.to_datetime(df_target['period_start']).dt.month

    df_target = df_target[['site', 'type', 'item', 'site_category', 'year','month']]

    df_target['type'] = df_target['type'].replace({'wzs_esgi':'ESGI'})

    year_value = df_target['year']
    month_value = df_target['month']
    site_value = df_target['site']
    item_value = df_target['item']

    if len(month_value) == 0 or len(site_value) == 0 or len(item_value) == 0:

        pass

    elif df_target.size ==0:

        pass

    else:


        delete_query = f"""DELETE FROM app.{target_table} WHERE site IN {tuple(site_value)} AND item IN {tuple(item_value)} AND year IN {tuple(year_value)} AND month IN {tuple(month_value)}"""
        conn = db_eco.connect()
        conn.execute(delete_query)

        df_target.to_sql(str(target_table), con=db_eco, schema='app', if_exists='append', index=False, chunksize=1000)
    conn.close()


def source_status(target_table):

    if dt.now().month == 1:

        year = dt.now().year-1
        year_last = dt.now().year-1
        month_start = 1
        month_end = 12
        month_last = 11
        period_year_start = date(dt.now().year-1, 1, 1).strftime("%Y-%m-%d")
        period_year_end = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")

    elif dt.now().month == 2:

        year = dt.now().year
        year_last = dt.now().year-1
        month_start = 1
        month_end = dt.now().month - 1
        month_last = 12
        period_year_start = date(dt.now().year, 1, 1).strftime("%Y-%m-%d")
        period_year_end = date(dt.now().year, dt.now().month - 1, 1).strftime("%Y-%m-%d")

    else:

        year = dt.now().year
        year_last = dt.now().year
        month_start = 1
        month_end = dt.now().month - 1
        month_last = dt.now().month - 2
        period_year_start = date(dt.now().year, 1, 1).strftime("%Y-%m-%d")
        period_year_end = date(dt.now().year, dt.now().month - 1, 1).strftime("%Y-%m-%d")

    if dt.now().day == 1:

        df_current = pd.read_sql(f"""SELECT site_category, site, item, "year", "month", confirm FROM app.{target_table} where year ={year_last} and month ={month_last}""",db_eco )
        df_current['year'] = year
        df_current['month'] = month_end
        df_current['type'] = ''


        delete_query1 = f"""DELETE FROM app.{target_table} WHERE year = {year} AND month ={month_end}"""

        conn = db_eco.connect()
        conn.execute(delete_query1)

        df_current.to_sql(str({target_table}), con=db_eco, schema='app',
                            if_exists='append', index=False, chunksize=1000)
        conn.close()

    df1 = pd.read_sql(f"""select distinct site ,period_start,type from raw.electricity_total_decarb where period_start >='{period_year_start}' and period_start <='{period_year_end}' and type !=''""", con=db_eco)

    df1['item'] = '實際用電'

    item_dict = {'光伏':'自建太陽能','綠電':'直購綠電','綠證':'購買綠證'}

    df2 = pd.read_sql(f"""select distinct site ,category2 as "item", period_start,type from raw.renewable_energy_decarb where period_start >='{period_year_start}' and period_start <='{period_year_end}' and type !=''""", con=db_eco)

    df2['item'] = df2['item'].replace(item_dict)

    df = df1.append(df2).reset_index(drop=True)

    site_mapping = pd.read_sql(f"""SELECT DISTINCT site_category, site FROM raw.plant_mapping """, con=db_eco)

    df_target = df.merge(site_mapping, on='site', how='left')

    df_target['year'] = pd.to_datetime(df_target['period_start']).dt.year

    df_target['month'] = pd.to_datetime(df_target['period_start']).dt.month

    df_target = df_target[['site', 'type', 'item', 'site_category', 'year','month']]

    df_orign = pd.read_sql(f"""SELECT site_category, site, item, "year", "month", confirm FROM app.{target_table} where year ={year} and month >={month_start} and month <={month_end}""",db_eco )

    df_status = df_orign.merge(df_target, on=['site','item','site_category','year','month'], how='left')

    df_status['type'] = df_status['type'].replace({'wzs_esgi':'ESGI'})
    df_status['type'] = df_status['type'].replace({'CSR':'ESG Database'})

    delete_query = f"""DELETE FROM app.{target_table} WHERE year = {year} AND month >={month_start} AND month <={month_end}"""

    conn = db_eco.connect()
    conn.execute(delete_query)

    df_status.to_sql(str(target_table), con=db_eco, schema='app',
                        if_exists='append', index=False, chunksize=1000)
    conn.close()



#


def import_actual_elect():

    if dt.now().month == 1:

        year = dt.now().year-1
        month_start = 1
        month_end = 12
        period_year_start = date(dt.now().year-1, 1, 1).strftime("%Y-%m-%d")
        period_year_end = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")

    else:

        year = dt.now().year
        month_start = 1
        month_end = 12
        period_year_start = date(dt.now().year, 1, 1).strftime("%Y-%m-%d")
        period_year_end = date(dt.now().year, 12, 1).strftime("%Y-%m-%d")


    df = pd.read_sql(f"""SELECT site, amount, period_start FROM staging.electricity_decarb where bo = 'ALL' and site !='ALL' and period_start >= '{period_year_start}' and period_start <= '{period_year_end}'""", db_eco)

    #get WIHK-1 and WIHK-2 from source table
    df_elect_csr = pd.read_sql(f"""SELECT plant as site, period_start,  indicatorvalue as amount FROM app.electricity_backstage_update WHERE period_start >= '{period_year_start}' AND period_start <= '{period_year_end}' AND indicatorvalue > 0 AND plant in ('WIHK-1','WIHK-2')""", con=db_eco)
    df_elect_esgi = pd.read_sql(f"""SELECT plant as site, period_start, amount FROM raw.electricity_total_wzsesgi WHERE period_start >= '{period_year_start}' AND period_start <= '{period_year_end}' AND  plant in ('WIHK-1','WIHK-2')""", con=db_eco)
    df_elect_compare = df_elect_csr.merge(df_elect_esgi,on = ['site','period_start'],how = 'outer')
    df_elect_compare = df_elect_compare.sort_values(by = ['site','period_start']).reset_index(drop = True)

    #replace ESGI with CSR
    for i in range(0,len(df_elect_compare)):
        if pd.isna(df_elect_compare['amount_x'][i]):
            df_elect_compare['amount_x'][i] = df_elect_compare['amount_y'][i]
    df_elect_compare['amount'] = df_elect_compare['amount_x']
    df_elect_compare = df_elect_compare[['site','period_start','amount']]

    df = df.append(df_elect_compare).reset_index(drop = True)

    #calculate YTM for elect_target_year
    df_all = df.groupby('site').sum().reset_index()

    df_version = pd.read_sql(f"""SELECT DISTINCT "version",sign_off_id,last_update_time FROM app.elect_target_month where "year" = {year} and category = 'predict' and "version" = (SELECT MAX("version") FROM app.elect_target_month where "year" = {year} and validate is true) """, db_eco)

    df['year'] = pd.to_datetime(df['period_start']).dt.year
    df['month'] = pd.to_datetime(df['period_start']).dt.month
    df['category'] = 'actual'
    df['validate'] = True
    df['version'] = df_version['version'][0]
    df['sign_off_id'] = df_version['sign_off_id'][0]
    df['last_update_time'] = df_version['last_update_time'][0]


    df_all['year'] = pd.to_datetime(df['period_start']).dt.year
    df_all['category'] = 'actual'
    df_all['validate'] = True
    df_all['version'] = df_version['version'][0]
    df_all['sign_off_id'] = df_version['sign_off_id'][0]
    df_all['last_update_time'] = df_version['last_update_time'][0]

    df = df[['site', 'amount', 'year', 'month', 'category','validate','version','sign_off_id','last_update_time']]
    df_all = df_all[['site', 'amount', 'year', 'category','validate','version','sign_off_id','last_update_time']]

    year_target = df['year']
    month_target = df['month']
    site_target = df['site']
    category_target = df['category']
    version_target = df['version']

    if len(month_target) == 0 or len(site_target) == 0:

        pass

    elif df.size == 0:

        pass

    else:

        delete_query = f"""DELETE FROM app.elect_target_month WHERE year IN {tuple(year_target)} AND month IN {tuple(month_target)} AND site IN {tuple(site_target)} AND category IN {tuple(category_target)} AND version IN {tuple(version_target)} AND validate is True"""

        conn = db_eco.connect()
        conn.execute(delete_query)

        df.to_sql('elect_target_month', con=db_eco, schema='app',
                        if_exists='append', index=False, chunksize=1000)
        
        delete_query = f"""DELETE FROM app.elect_target_year WHERE year IN {tuple(year_target)} AND site IN {tuple(site_target)} AND category IN {tuple(category_target)} AND version IN {tuple(version_target)} AND validate is True"""

        conn.execute(delete_query)

        df_all.to_sql('elect_target_year', con=db_eco, schema='app',
                        if_exists='append', index=False, chunksize=1000)
        
        conn.close()




