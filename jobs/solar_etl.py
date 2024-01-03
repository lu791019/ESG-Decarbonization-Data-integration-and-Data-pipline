import os
from datetime import datetime

from factories.source_to_raw_factory import main as source_to_raw
from jobs.raw_to_staging import raw_to_staging
from jobs.staging_to_app import staging_to_app
from jobs.wzsesgi_etl import esgi2solar
from services.mail_service import MailService


def get_stage():
    return os.environ['FLASK_ENV'] if 'FLASK_ENV' in os.environ else 'development'


def solar_etl():

    stage = get_stage()

    try:
        esgi2solar()
        # source_to_raw('solar')

        raw_to_staging('solar', stage)
        raw_to_staging('solar_remain', stage)
        raw_to_staging('solar_other', stage)
        raw_to_staging('solar_info', stage)

        staging_to_app('solar_energy_overview', stage)

        # temp
        mail = MailService(
            '[success][{}] decarb-etl remaining elec job report successed.'.format(stage))
        mail.send_text('success')

    except Exception as inst:
        mail = MailService(
            '[failed][{}] decarb-etl remaining elec job report failed.'.format(stage))
        mail.send_text('failed: {}'.format(inst))
