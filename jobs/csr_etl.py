import calendar
from datetime import date
from datetime import datetime as dt
from datetime import timedelta

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import *

from models import engine, engine_source
from services.mail_service import MailService

connect_eco_string = engine.get_connect_string()

db_eco = create_engine(connect_eco_string, echo=True)

connect_eco_string_csr = engine_source.get_connect_string_csr()

db_csr = create_engine(connect_eco_string_csr, echo=True)

def office2raw(raw_table):

    if raw_table =='electricity_total_decarb':

        df_office = pd.read_sql(f"""SELECT plant as site, amount, unit, period_start,"type" FROM raw.electricity_office""", db_eco)

        if df_office.shape[0] > 0:

            conn = db_eco.connect()
            site_list = df_office['site'].unique()
            for i in range(0,len(site_list)):
                df_office_site = df_office[df_office['site']  == site_list[i]]
                period_start = df_office_site['period_start'].unique()
                period_start_list = "','".join(period_start.astype(str))

                conn.execute(f"DELETE FROM raw.electricity_total_decarb WHERE site = '{site_list[i]}' AND period_start IN ('{period_start_list}')")
                df_office_site.to_sql('electricity_total_decarb', conn, index=False,  if_exists='append', schema='raw', chunksize=10000)

            conn.close()


def csr_replace(schema, raw, csr):

    start_date = dt(2023, 1, 1)

    end_date = dt(dt.now().year, dt.now().month, 1)

    period_start = start_date

    while period_start <= end_date:

        if raw =='electricity_total_decarb':

            site_dict = {'WIHK-1':'WIHK','WIHK-2':'WIHK','WMY-1':'WMY','WMY-2':'WMY','WCD-1':'WCD','WCD-2':'WCD','WCCD':'WCD'}

            df_elect_csr = pd.read_sql(f"""SELECT plant as site, period_start,  indicatorvalue as amount FROM app.electricity_backstage_update WHERE period_start = '{period_start}' AND indicatorvalue > 0 AND plant not in ('WHC','WMCQ')""", con=db_eco)

            df_elect_csr = df_elect_csr.replace({'site': site_dict})

            df_elect_csr = df_elect_csr.groupby(['site', 'period_start']).sum().reset_index()

            df_elect_csr_WZKS = pd.read_sql(f"""SELECT sitename as site, indicatorvalue as amount,  period_start FROM raw.csr_electricity_indicator  WHERE indicatorvalue > 0 AND sitename in ('WZS','WKS')""", con=db_eco)
            df_elect_csr_WZKS = df_elect_csr_WZKS[df_elect_csr_WZKS['period_start'] == str(period_start)].reset_index(drop=True)

            df_elect_csr = df_elect_csr.append(df_elect_csr_WZKS)

            df_elect_csr['unit'] = '度'

            df_elect_csr['type'] = 'CSR'

            if df_elect_csr.shape[0] > 0:
                conn = db_eco.connect()
                site_list = df_elect_csr['site'].unique()
                site_list = "','".join(site_list)
                conn.execute(f"DELETE FROM raw.electricity_total_decarb WHERE site IN ('{site_list}') AND period_start = '{period_start}'")
                df_elect_csr.to_sql('electricity_total_decarb', conn, index=False,  if_exists='append', schema='raw', chunksize=10000)

                conn.close()


        if raw =='renewable_energy_decarb':

            site_dict = {'WIHK1':'WIHK','WIHK2':'WIHK','WMYP1':'WMY','WMYP2':'WMY'}

            category2_dict = {'轉供綠電總電量':'綠電','轉供綠電電量':'綠電'}

            df_grenergy_csr = pd.read_sql(f"""SELECT indicatormonth as month , indicatorname as category2, indicatorvalue as amount, indicatoryear as year, sitename as site FROM raw.whq_esgcsrdatabase_view_csrindicatordetail_all where indicatorname in ('轉供綠電總電量','轉供綠電電量') AND indicatorvalue > 0""", con=db_eco)

            df_grenergy_csr.dropna(inplace=True)

            df_grenergy_csr = df_grenergy_csr.replace({'site': site_dict,'category2':category2_dict})

            df_grenergy_csr['period_start'] = pd.to_datetime(df_grenergy_csr['year'] + '-' + df_grenergy_csr['month'] + '-01')

            df_grenergy_csr['category1'] = '綠色能源'

            df_grenergy_csr['unit'] = '度'

            df_grenergy_csr['type'] = 'CSR'

            df_grenergy_csr = df_grenergy_csr[['category1', 'category2', 'amount','site', 'period_start', 'type', 'unit']]

            df_grenergy_csr = df_grenergy_csr[df_grenergy_csr['period_start'] == str(period_start)].reset_index(drop=True)

            if df_grenergy_csr.shape[0] > 0:
                conn = db_eco.connect()
                site_list = df_grenergy_csr['site'].unique()
                for i in range(0,len(site_list)):
                    df_grenergy_csr_site = df_grenergy_csr[df_grenergy_csr['site']  == site_list[i]]
                    category2_list = df_grenergy_csr_site['category2'].unique()
                    category2_list = "','".join(category2_list)

                    conn.execute(f"DELETE FROM raw.renewable_energy_decarb WHERE site = '{site_list[i]}' AND category2 IN ('{category2_list}') AND period_start = '{period_start}'")
                    df_grenergy_csr_site.to_sql('renewable_energy_decarb', conn, index=False,  if_exists='append', schema='raw', chunksize=10000)

                conn.close()

        period_start += relativedelta(months=1)


def csr_solar_replace(target_table,stage):

    try:

        site_dict = {'WIHK1':'WIHK','WIHK2':'WIHK','WMYP1':'WMY','WMYP2':'WMY'}
        category2_dict = {'太陽能發電量':'光伏'}

        start_date = dt(2023, 1, 1)

        end_date = dt(dt.now().year, dt.now().month, 1)

        period_start = start_date

        while period_start <= end_date:

        # if dt.now().month == 1:

        #     year = dt.now().year-1
        #     month_start = 1
        #     month_end = 12
        #     period_start = date(dt.now().year-1, 1, 1).strftime("%Y-%m-%d")
        #     period_end = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")

        # else:

        #     year = dt.now().year
        #     month_start = 1
        #     month_end = dt.now().month - 1
        #     period_start = date(dt.now().year, 1, 1).strftime("%Y-%m-%d")
        #     period_end = date(dt.now().year, dt.now().month-1, 1).strftime("%Y-%m-%d")

            df_solar_csr1 = pd.read_sql(f"""SELECT indicatormonth as month , indicatorname as category2, indicatorvalue as amount, indicatoryear as year, sitename as site FROM raw.whq_esgcsrdatabase_view_csrindicatordetail_all where sitename in ('WZS','WOK','WKS') and indicatorname in ('太陽能發電量') AND indicatorvalue > 0""", con=db_eco)
            df_solar_csr1.dropna(inplace=True)
            df_solar_csr1 = df_solar_csr1.replace({'site': site_dict,'category2':category2_dict})

            df_solar_csr2 = pd.read_sql(f"""SELECT SiteName as site, EvidenceY as year, EvidenceM as month, RawDataValue as amount FROM CSSR.dbo.View_RawDataDetail_EcoSsot WHERE rawdataname LIKE '%太陽能%' and SiteName not in ('WZS','WOK','WKS') and RawDataValue > 0""", con=db_csr)
            df_solar_csr2 = df_solar_csr2.groupby(['site', 'year','month']).sum().reset_index()
            df_solar_csr2['category2'] = '光伏'
            df_solar_csr2.dropna(inplace=True)
            df_solar_csr2 = df_solar_csr2.replace({'site': site_dict})

            df_solar_csr = df_solar_csr1.append(df_solar_csr2).reset_index(drop=True)

            df_solar_csr['period_start'] = pd.to_datetime(df_solar_csr['year'] + '-' + df_solar_csr['month'] + '-01')

            df_solar_csr['category1'] = '綠色能源'

            df_solar_csr['unit'] = '度'

            df_solar_csr['type'] = 'CSR'

            df_solar_csr = df_solar_csr[['category1', 'category2', 'amount','site', 'period_start', 'type', 'unit']]

            df_solar_csr = df_solar_csr[(df_solar_csr['period_start'] == str(period_start))].reset_index(drop=True)


            if df_solar_csr.shape[0] > 0:

                conn = db_eco.connect()
                site_list = df_solar_csr['site'].unique()
                for i in range(0,len(site_list)):

                    df_solar_csr_site = df_solar_csr[df_solar_csr['site']  == site_list[i]]

                    category2 = df_solar_csr_site['category2'].unique()
                    category2_list = "','".join(category2.astype(str))

                    conn.execute(f"DELETE FROM raw.{target_table} WHERE site = '{site_list[i]}' AND period_start IN ('{period_start}') AND category2 IN ('{category2_list}')")
                    df_solar_csr_site.to_sql(str(target_table), conn, index=False,  if_exists='append', schema='raw', chunksize=10000)

                conn.close()

            period_start += relativedelta(months=1)

    except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] csr_solar_replace etl info cron job report:'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error


def csr_rawsolar_replace(target_table,stage):

    try:

        plant_dict = {'WIHK1':'WIHK','WIHK2':'WIHK','WMYP1':'WMY','WMYP2':'WMY'}

        start_date = dt(2023, 1, 1)

        end_date = dt(dt.now().year, dt.now().month, 1)

        period_start = start_date

        while period_start <= end_date:

        # if dt.now().month == 1:

        #     year = dt.now().year-1
        #     month_start = 1
        #     month_end = 12
        #     period_start = date(dt.now().year-1, 1, 1).strftime("%Y-%m-%d")
        #     period_end = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")

        # else:

        #     year = dt.now().year
        #     month_start = 1
        #     month_end = dt.now().month - 1
        #     period_start = date(dt.now().year, 1, 1).strftime("%Y-%m-%d")
        #     period_end = date(dt.now().year, dt.now().month-1, 1).strftime("%Y-%m-%d")

            df_solar_csr1 = pd.read_sql(f"""SELECT indicatormonth as month , indicatorvalue as amount, indicatoryear as year, sitename as plant FROM raw.whq_esgcsrdatabase_view_csrindicatordetail_all where sitename in ('WZS','WOK','WKS') and indicatorname in ('太陽能發電量') AND indicatorvalue > 0""", con=db_eco)
            df_solar_csr1.dropna(inplace=True)
            df_solar_csr1 = df_solar_csr1.replace({'plant': plant_dict})

            df_solar_csr2 = pd.read_sql(f"""SELECT SiteName as plant, EvidenceY as year, EvidenceM as month, RawDataValue as amount FROM CSSR.dbo.View_RawDataDetail_EcoSsot WHERE rawdataname LIKE '%太陽能%' and SiteName not in ('WZS','WOK','WKS') and RawDataValue > 0""", con=db_csr)
            df_solar_csr2 = df_solar_csr2.groupby(['plant', 'year','month']).sum().reset_index()
            df_solar_csr2.dropna(inplace=True)
            df_solar_csr2 = df_solar_csr2.replace({'plant': plant_dict})

            df_solar_csr = df_solar_csr1.append(df_solar_csr2).reset_index(drop=True)

            df_solar_csr['period_start'] = pd.to_datetime(df_solar_csr['year'] + '-' + df_solar_csr['month'] + '-01')

            df_solar_csr['category'] = 'actual'

            df_solar_csr['type'] = 'CSR'

            df_solar_csr = df_solar_csr[['category', 'amount','plant', 'period_start', 'type']]

            df_solar_csr = df_solar_csr[(df_solar_csr['period_start'] == str(period_start))].reset_index(drop=True)


            if df_solar_csr.shape[0] > 0:

                conn = db_eco.connect()
                plant_list = df_solar_csr['plant'].unique()
                for i in range(0,len(plant_list)):

                    df_solar_csr_site = df_solar_csr[df_solar_csr['plant']  == plant_list[i]]

                    category = df_solar_csr_site['category'].unique()
                    category_list = "','".join(category.astype(str))

                    conn.execute(f"DELETE FROM raw.{target_table} WHERE plant = '{plant_list[i]}' AND period_start IN ('{period_start}') AND category IN ('{category_list}')")
                    df_solar_csr_site.to_sql(str(target_table), conn, index=False,  if_exists='append', schema='raw', chunksize=10000)

                conn.close()

            period_start += relativedelta(months=1)

    except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] csr_solar_replace etl info cron job report:'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

