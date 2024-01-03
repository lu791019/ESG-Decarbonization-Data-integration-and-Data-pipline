from flask import Blueprint

from app.renewable_energy.sim import sim_bp

renewable_energy = Blueprint(
    'renewable_energy', __name__, url_prefix='/renewable_energy')
renewable_energy.register_blueprint(sim_bp)
