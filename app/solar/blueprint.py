from flask import Blueprint

from app.solar.remaining_power import remaining_power_bp

solar = Blueprint('solar', __name__, url_prefix='/solar')
solar.register_blueprint(remaining_power_bp)
