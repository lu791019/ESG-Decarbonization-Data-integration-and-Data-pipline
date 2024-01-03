import pandas as pd
import numpy as np
from datetime import datetime as dt, date, timedelta
from sqlalchemy import *
from models import engine, engine_source
import calendar
from dateutil.relativedelta import relativedelta

connect_eco_string = engine.get_connect_string()

db_eco = create_engine(connect_eco_string, echo=True)

# def update_esgi_data(df_target,schema, target_table):

#     conn = db_eco.connect()
#     conn.execute(f"""TRUNCATE TABLE {schema}.{target_table}""")

#     df_target.to_sql(target_table, con=db_eco, schema=str(schema), if_exists='append', index=False, chunksize=1000)
#     conn.close()


def category_group(df,data_name):

    df_target = pd.DataFrame()

    for i in data_name:
        df_target = pd.concat([df_target, df[df['data_name'] == i]])

    return df_target


def insert_col(df,unit):
    df['unit'] = str(unit)
    df['type'] = 'wzs_esgi'

    return df

def preprocess_df(df):

    plant_dict = {'LCM-1':'WOK','LCM-2':'WTZ','WIH-1':'WIH'}

    df = df.replace({'plant': plant_dict})

    df['plant'] = df['plant'].replace(plant_dict)

    df['amount'] = df['amount'].replace('NA', np.nan)

    df['amount'] = df['amount'].astype(float)

    df['amount'].fillna(0, inplace=True)

    return df


def esgi2raw():

    plant_dict = {'LCM-1':'WOK','LCM-2':'WTZ','WIH-1':'WIH'}
    value_change = {'NA': pd.NA}

    df = pd.read_sql(f"""SELECT data_name, plant  , period_start,  data_value as amount FROM raw.wzs_esgi_environment_indicator_item where plant not in ('WCD','WZS','WKS')""", db_eco)

    df_elect_esgi = pd.read_sql(f"""SELECT data_name, plant , period_start,  data_value as amount FROM raw.wzs_esgi_environment_indicator_item
    where plant not in ('WCD','WZS','WKS') and performance_goalsid = 4""", db_eco)

    plant_mapping = pd.read_sql('SELECT DISTINCT plant_name AS "plant",site FROM raw.plant_mapping', con=db_eco)


    if df.size !=0:

        df = preprocess_df(df)

        df_elect_esgi = preprocess_df(df_elect_esgi)

        df_elect_esgi['amount'] = df_elect_esgi['amount'] * 1000

        df_elect = category_group(df_elect_esgi,['總用電度數'])

        df_renew = category_group(df,['綠電電量','購買綠證電量','自建自用電量'])

    if df_elect.size !=0:

        df_elect = insert_col(df_elect,'度')

        df_elect = df_elect[[ 'plant', 'period_start', 'amount', 'unit', 'type']]

        df_elect.drop_duplicates(inplace=True)

        df_elect_site = df_elect.merge(plant_mapping, on='plant', how='left')

        df_elect_site = df_elect_site[['site','period_start', 'amount', 'unit', 'type']]

        df_elect_site = df_elect_site.groupby(['site','period_start', 'unit', 'type']).sum().reset_index()

        period_start_value = df_elect_site['period_start']
        site_value = df_elect_site['site']

        if len(period_start_value) == 0 or len(site_value) == 0:

                pass

        elif df_elect_site.size ==0:

            pass

        elif df_elect_site.shape[0] == 1:

            delete_query = f"""DELETE FROM raw.electricity_total_decarb WHERE site = '{site_value[0]}' AND period_start = '{period_start_value[0]}'"""

            conn = db_eco.connect()
            conn.execute(delete_query)

            df_elect_site.to_sql('electricity_total_decarb', con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)
            conn.close()

        else:

            delete_query = f"""DELETE FROM raw.electricity_total_decarb WHERE site IN {tuple(site_value)} AND period_start IN {tuple(str(date) for date in period_start_value)}"""

            conn = db_eco.connect()
            conn.execute(delete_query)

            df_elect_site.to_sql('electricity_total_decarb', con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)
            conn.close()

        # update_esgi_data(df_elect_site,'raw', 'electricity_total_decarb')



    if df_renew.size !=0:

        df_renew = insert_col(df_renew,'度')

        df_renew['category1'] = '綠色能源'

        df_renew.rename(columns={'data_name': 'category2'}, inplace=True)

        df_renew['category2'] = df_renew['category2'].replace({'綠電電量':'綠電','購買綠證電量':'綠證', '自建自用電量':'光伏'})

        df_renew = df_renew[[ 'category1', 'category2', 'plant', 'period_start', 'amount', 'unit', 'type']]

        df_renew.drop_duplicates(inplace=True)

        df_renew_site = df_renew.merge(plant_mapping, on='plant', how='left')

        df_renew_site = df_renew_site[['category1', 'category2', 'period_start', 'amount', 'unit','type', 'site']]

        df_renew_site = df_renew_site.groupby(['category1', 'category2', 'period_start', 'unit','type', 'site']).sum().reset_index()

        # update_esgi_data(df_renew_site,'raw', 'renewable_energy_decarb')

        period_start_value = df_renew_site['period_start']
        site_value = df_renew_site['site']
        category2_value = df_renew_site['category2']

        if len(period_start_value) == 0 or len(site_value) == 0:

            pass

        elif df_renew_site.size ==0:

            pass

        elif df_renew_site.shape[0] == 1:

            delete_query = f"""DELETE FROM raw.renewable_energy_decarb WHERE site = '{site_value[0]}' AND category2 = '{category2_value[0]}' AND period_start = '{period_start_value[0]}'"""

            conn = db_eco.connect()
            conn.execute(delete_query)

            df_renew_site.to_sql('renewable_energy_decarb', con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)
            conn.close()

        else:

            delete_query = f"""DELETE FROM raw.renewable_energy_decarb WHERE site IN {tuple(site_value)} AND category2 IN {tuple(category2_value)} AND period_start IN {tuple(str(date) for date in period_start_value)}"""

            conn = db_eco.connect()
            conn.execute(delete_query)

            df_renew_site.to_sql('renewable_energy_decarb', con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)
            conn.close()


def esgi2solar():

    plant_dict = {'WVN-1':'WVN','WVN-2':'WVN','WMY-1':'WMY'}
    value_change = {'NA': pd.NA}

    df = pd.read_sql(f"""SELECT data_name, plant  , period_start,  data_value as amount FROM raw.wzs_esgi_environment_indicator_item where plant not in ('WCD','WZS','WKS') """, db_eco)

    df = preprocess_df(df)

    df_solar = category_group(df,['自建自用電量'])

    df_solar = insert_col(df_solar,'度')

    df_solar['category'] = 'actual'

    df_solar = df_solar.replace({'plant': plant_dict})

    df_solar = df_solar[['category','plant', 'period_start', 'amount', 'type']]

    df_solar.drop_duplicates(inplace=True)

    df_solar_actual = df_solar[df_solar['amount']>0]

    df_solar_actual = df_solar_actual.groupby(['category', 'plant', 'period_start','type']).sum().reset_index()

    period_start_value = df_solar_actual['period_start']
    plant_value = df_solar_actual['plant']
    type_value = df_solar_actual['type']
    category_value = df_solar_actual['category']


    if len(period_start_value) == 0 or len(plant_value) == 0:

            pass

    elif df_solar_actual.size ==0:

        pass

    else:

        delete_query = f"""DELETE FROM raw.solar WHERE plant IN {tuple(plant_value)} AND category IN {tuple(category_value)} AND period_start IN {tuple(str(date) for date in period_start_value)}"""

        conn = db_eco.connect()
        conn.execute(delete_query)

        df_solar_actual.to_sql('solar', con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)
        conn.close()



