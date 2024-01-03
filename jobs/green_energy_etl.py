import os
from datetime import datetime

from app.celery import app
from jobs.raw_to_staging import raw_to_staging
from jobs.renew_green_energy import cal_renew_total, green_energy_overview
from jobs.staging_to_app import staging_to_app


def get_stage():
    return os.environ['FLASK_ENV'] if 'FLASK_ENV' in os.environ else 'development'


@app.task(name='green-certificate-update')
def green_energy_etl():

    green_energy_overview('add_customer_data')

    green_energy_overview('summarize_all_data')

    # temp
