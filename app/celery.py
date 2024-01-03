import sqlalchemy as sa
from celery import Celery

from config import Config

broker_url = Config.CELERY_BROKER_URL
result_backend = "db+{}".format(sa.engine.URL.create(
    drivername="postgresql+psycopg2",
    username=Config.ECO_SSOT_RDS_USERNAME,
    password=Config.ECO_SSOT_RDS_PASSWORD,
    host=Config.ECO_SSOT_RDS_HOST,
    port=int(Config.ECO_SSOT_RDS_PORT) if Config.ECO_SSOT_RDS_PORT else 5432,
    database=Config.ECO_SSOT_RDS_DATABASE,
))

app = Celery('app',
             broker=broker_url,
             backend=result_backend,
             include=['Model.Factory_elect_simulator', 'jobs.example',
                      'jobs.shipments_etl', 'jobs.renew_setting_etl', 'Model.RE_purpose_optimizer',
                      'elec_transfer.upload_excel_to_DB', 'jobs.green_energy_etl',
                      'services.mail_service', 'app.sign_off.blueprint'],
             broker_connection_retry_on_startup=True
             )

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
    result_extended=True,
    task_track_started=True,
)

if __name__ == '__main__':
    app.start()
