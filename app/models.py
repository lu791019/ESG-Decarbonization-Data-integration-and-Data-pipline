from datetime import datetime, timedelta

from .extensions import Base, db


class bbb(db.Model):
    __tablename__ = 'bbb'
    __table_args__ = {"schema": "public"}
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128))
    test = db.Column(db.Integer)

    def __init__(self, id, name, test):
        self.id = id
        self.name = name
        self.test = test


class GreenElectSimulate(db.Model):
    __tablename__ = 'green_elect_simulate'
    __table_args__ = {"schema": "app"}

    id = db.Column(db.Integer, primary_key=True)
    area = db.Column(db.String)
    site = db.Column(db.String)
    year = db.Column(db.Integer)
    predict_roc = db.Column(db.Float)
    amount = db.Column(db.Float)
    green_full_ratio = db.Column(db.Float)
    last_update_time = db.Column(db.DateTime, server_default=db.func.now())


class GreenElectCostRatio(db.Model):
    __tablename__ = 'green_elect_cost_ratio'
    __table_args__ = {"schema": "app"}

    id = db.Column(db.Integer, primary_key=True)
    area = db.Column(db.String)
    year = db.Column(db.Integer)
    category = db.Column(db.String)
    amount = db.Column(db.Float)
    last_update_time = db.Column(db.DateTime, server_default=db.func.now())


class GreenEnergyRequest(db.Model):
    __tablename__ = 'green_energy_request'
    __table_args__ = {"schema": "app"}

    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer)
    site = db.Column(db.String)
    plant = db.Column(db.String)
    customer = db.Column(db.String)
    pic = db.Column(db.String)
    solar_confirm = db.Column(db.Boolean)
    solar_remark = db.Column(db.String)
    hydroelect_confirm = db.Column(db.Boolean)
    hydroelect_remark = db.Column(db.String)
    re_2023_rate = db.Column(db.Float)
    re_2024_rate = db.Column(db.Float)
    re_2030_rate = db.Column(db.Float)
    last_update_time = db.Column(db.DateTime, server_default=db.func.now())


class GreenPurchase(db.Model):
    __tablename__ = 'green_purchase'
    __table_args__ = {"schema": "app"}

    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer)
    site = db.Column(db.String)
    customer = db.Column(db.String)
    quarter = db.Column(db.String)
    provider = db.Column(db.String)
    unit_price = db.Column(db.Float)
    amount = db.Column(db.Float)
    total_price = db.Column(db.Float)
    last_update_time = db.Column(db.DateTime, default=db.func.now())


class GreenAmount(db.Model):
    __tablename__ = 'green_amount'
    __table_args__ = {"schema": "app"}

    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer)
    quarter = db.Column(db.String)
    priority = db.Column(db.Integer)
    area = db.Column(db.String)
    site = db.Column(db.String)
    customer = db.Column(db.String)
    unit_price = db.Column(db.Float)
    elect_amount = db.Column(db.Float)
    renew_target = db.Column(db.Float)
    solar_amount = db.Column(db.Float)
    green_amount = db.Column(db.Float)
    grey_amount = db.Column(db.Float)
    last_update_time = db.Column(db.DateTime, default=db.func.now())


# class DecarbElecOverview(db.Model):
#     __tablename__ = 'decarb_elec_overview'
#     __table_args__ = { "schema":"public" }

#     id = db.Column(db.Integer, primary_key=True)
#     year = db.Column(db.Integer)
#     month = db.Column(db.Integer)
#     category = db.Column(db.String)
#     ytm_amount = db.Column(db.Float)
#     test = db.Column(db.Integer)
#     last_update_time = db.Column(db.DateTime, default=datetime.now)


#     def __init__(self,id, year, month, category, ytm_amount,test, last_update_time=datetime.now):
#         self.id = id
#         self.year = year
#         self.month = month
#         self.category = category
#         self.ytm_amount = ytm_amount
#         self.test = test
#         self.last_update_time = last_update_time

    # def __repr__(self):
    #     return f"<DecarbElecOverview(id={self.id}, year={self.year}, month={self.month}, category={self.category}, ytm_amount={self.ytm_amount},test={self.test} last_update_time={self.last_update_time})>"

class CeleryTaskmeta(db.Model):
    __tablename__ = 'celery_taskmeta'
    __table_args__ = {"schema": "public"}

    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.String(155), unique=True)
    status = db.Column(db.String(50))
    result = db.Column(db.LargeBinary)
    date_done = db.Column(db.DateTime)
    traceback = db.Column(db.Text)
    name = db.Column(db.String(155))
    args = db.Column(db.LargeBinary)
    kwargs = db.Column(db.LargeBinary)
    worker = db.Column(db.String(155))
    retries = db.Column(db.Integer)
    queue = db.Column(db.String(155))

    def __init__(self, id, task_id, status, result, date_done, traceback, name, args, kwargs, worker, retries, queue):
        self.id = id
        self.task_id = task_id
        self.status = status
        self.result = result
        self.date_done = date_done
        self.traceback = traceback
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.worker = worker
        self.retries = retries
        self.queue = queue


class CeleryTasksetmeta(db.Model):
    __tablename__ = 'celery_tasksetmeta'
    __table_args__ = {"schema": "public"}

    id = db.Column(db.Integer, primary_key=True)
    taskset_id = db.Column(db.String(155), unique=True)
    result = db.Column(db.LargeBinary)
    date_done = db.Column(db.DateTime)

    def __init__(self, id, taskset_id, result, date_done):
        self.id = id
        self.taskset_id = taskset_id
        self.result = result
        self.date_done = date_done


# class DecarbElecSimulate(db.Model):
#     __tablename__ = 'decarb_elect_simulate'
#     __table_args__ = {"schema": "app"}

#     id = db.Column(db.Integer, primary_key=True)
#     site = db.Column(db.String)
#     year = db.Column(db.Integer)
#     version = db.Column(db.String)
#     amount = db.Column(db.Float)
#     last_update_time = db.Column(db.DateTime)
#     version_year = db.Column(db.Integer)

#     def __init__(self, site, year, version, amount, last_update_time=None, version_year=None):
#         self.id = id
#         self.site = site
#         self.year = year
#         self.version = version
#         self.amount = amount
#         self.last_update_time = last_update_time or datetime.now()
#         self.version_year = version_year

#     def __repr__(self):
#         return f"<DecarbElecSimulate(id={self.id}, site={self.site}, year={self.year}, version={self.version}, amount={self.amount}, last_update_time={self.last_update_time}, version_year={self.version_year})>"

class DecarbSignOff(Base):
    __tablename__ = 'decarb_sign_off'
    __table_args__ = {"schema": "app"}

    id = db.Column(db.String, primary_key=True, nullable=True)
    type = db.Column(db.String)
    year = db.Column(db.Integer)
    month = db.Column(db.Integer)
    status = db.Column(db.Integer)
    pic = db.Column(db.String)
    pic_message = db.Column(db.Text)
    reviewer = db.Column(db.String)
    reviewer_message = db.Column(db.Text)
    updated_at = db.Column(db.DateTime, default=db.func.now())
    created_at = db.Column(db.DateTime, default=db.func.now())

    def to_dict(self):
        return {
            "id": self.id,
            "type": self.type,
            "year": self.year,
            "month": self.month,
            "status": self.status,
            "pic": self.pic,
            "pic_message": self.pic_message,
            "reviewer": self.reviewer,
            "reviewer_message": self.reviewer_message,
            "updated_at": self.updated_at,
            "created_at": self.created_at,
        }


class DecarbElectSimulate(Base):
    __tablename__ = 'decarb_elect_simulate'
    __table_args__ = {"schema": "app"}

    id = db.Column(db.Integer, primary_key=True)
    site = db.Column(db.String)
    year = db.Column(db.Integer)
    version = db.Column(db.String)
    amount = db.Column(db.Float)
    last_update_time = db.Column(db.DateTime)
    version_year = db.Column(db.Integer)
    validate = db.Column(db.Boolean, default=True)
    sign_off_id = db.Column(db.String, db.ForeignKey('app.decarb_sign_off.id'))

    def __init__(self, site, year, version, amount, last_update_time=None, version_year=None, validate=True, sign_off_id=None):
        self.id = id
        self.site = site
        self.year = year
        self.version = version
        self.amount = amount
        self.last_update_time = last_update_time or datetime.now()
        self.version_year = version_year
        self.validate = validate
        self.sign_off_id = sign_off_id

    def __repr__(self):
        return f"<DecarbElecSimulate(id={self.id}, site={self.site}, year={self.year}, version={self.version}, amount={self.amount}, last_update_time={self.last_update_time}, version_year={self.version_year}, validate={self.validate}, sign_off_id={self.sign_off_id})>"


class ElectTargetYear(Base):
    __tablename__ = 'elect_target_year'
    __table_args__ = {"schema": "app"}

    id = db.Column(db.Integer, primary_key=True)
    site = db.Column(db.String)
    year = db.Column(db.Integer)
    category = db.Column(db.String)
    amount = db.Column(db.Float)
    last_update_time = db.Column(db.DateTime)
    version = db.Column(db.Integer)
    validate = db.Column(db.Boolean, default=True)
    sign_off_id = db.Column(db.String, db.ForeignKey('app.decarb_sign_off.id'))

    def __init__(self, site, year, category, amount, last_update_time=None, version=None, validate=True, sign_off_id=None):
        self.id = id
        self.site = site
        self.year = year
        self.category = category
        self.amount = amount
        self.last_update_time = last_update_time or datetime.now()
        self.version = version
        self.validate = validate
        self.sign_off_id = sign_off_id

    def __repr__(self):
        return f"<ElectTargetYear(id={self.id}, site={self.site}, year={self.year}, category={self.category}, amount={self.amount}, last_update_time={self.last_update_time}, version={self.version}, validate={self.validate}, sign_off_id={self.sign_off_id})>"


class ElectTargetMonth(Base):
    __tablename__ = 'elect_target_month'
    __table_args__ = {"schema": "app"}

    id = db.Column(db.Integer, primary_key=True)
    site = db.Column(db.String)
    category = db.Column(db.String)
    month = db.Column(db.Integer)
    amount = db.Column(db.Float)
    year = db.Column(db.Integer)
    version = db.Column(db.String)
    last_update_time = db.Column(db.DateTime)
    validate = db.Column(db.Boolean, default=True)
    sign_off_id = db.Column(db.String, db.ForeignKey('app.decarb_sign_off.id'))

    def __init__(self, site, category, month, amount, year, version, last_update_time=None, validate=True, sign_off_id=None):
        self.id = id
        self.site = site
        self.category = category
        self.month = month
        self.amount = amount
        self.year = year
        self.version = version
        self.last_update_time = last_update_time or datetime.now()
        self.validate = validate
        self.sign_off_id = sign_off_id

    def __repr__(self):
        return f"<ElecTargetMonth(id={self.id}, site={self.site}, category={self.category}, month={self.month}, amount={self.amount}, year={self.year}, version={self.version}, last_update_time={self.last_update_time}, validate={self.validate}, sign_off_id={self.sign_off_id})>"
