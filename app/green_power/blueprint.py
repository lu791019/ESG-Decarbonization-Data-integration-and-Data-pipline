from flask import Blueprint

from app.green_power.reconciliation import reconciliation_bp

green_power = Blueprint('green_power', __name__, url_prefix='/green_power')
green_power.register_blueprint(reconciliation_bp)
