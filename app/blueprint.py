from .certificate.blueprint import certificate
from .electricity.blueprint import electricity
from .green_power.blueprint import green_power
from .notification.blueprint import notification
from .renewable_energy.blueprint import renewable_energy
from .sign_off.blueprint import sign_off
from .solar.blueprint import solar
from .task.blueprint import task


def register(app):
    app.register_blueprint(electricity)  # 總用電量管理
    app.register_blueprint(solar)
    app.register_blueprint(certificate)
    app.register_blueprint(renewable_energy)  # 再生能源管理
    app.register_blueprint(green_power)  # 直購綠電管理
    app.register_blueprint(notification)
    app.register_blueprint(task)
    app.register_blueprint(sign_off)

    return
