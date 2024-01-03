import sqlalchemy as sa
from flasgger import Swagger
from flask import Flask

from config import Config

from .extensions import db, migrate
from .models import *


def create_app():
    app = Flask(__name__)

    app.config['SQLALCHEMY_DATABASE_URI'] = sa.engine.URL.create(
        drivername="postgresql+psycopg2",
        username=Config.ECO_SSOT_RDS_USERNAME,
        password=Config.ECO_SSOT_RDS_PASSWORD,
        host=Config.ECO_SSOT_RDS_HOST,
        port=int(Config.ECO_SSOT_RDS_PORT) if Config.ECO_SSOT_RDS_PORT else 5432,
        database=Config.ECO_SSOT_RDS_DATABASE,
    )
    app.config['SQLALCHEMY_DEFAULT_SCHEMA'] = 'app'

    db.init_app(app)
    migrate.init_app(app, db)

    app.config['SWAGGER'] = {
        "title": "Decarb ETL",
        "description": "Decarb ETL",
        "version": "0.0.1",
        "termsOfService": "",
        "hide_top_bar": True
    }

    Swagger(app)

    return app
