from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import declarative_base

db = SQLAlchemy()
# migrate = Migrate(include_schemas=True)
migrate = Migrate()


Base = declarative_base()
