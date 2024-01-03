""" 總用電量管理 """
from flask import Blueprint

from app.electricity.shipment import shipment_bp
from app.electricity.summary import summary_bp

electricity = Blueprint('electricity', __name__, url_prefix='/electricity')


# 出貨量變量維護
electricity.register_blueprint(shipment_bp)
# 總用電量總攬
electricity.register_blueprint(summary_bp)
