import json
from datetime import datetime, timezone

from flask_login import UserMixin
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import check_password_hash, generate_password_hash

db = SQLAlchemy()


def now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


class JsonMixin:
    @staticmethod
    def _load(v, default):
        try:
            return json.loads(v) if v else default
        except (TypeError, ValueError):
            return default


class Company(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False, unique=True)
    name_th = db.Column(db.String(200), default="")
    address = db.Column(db.Text, default="")
    logo_path = db.Column(db.String(500), default="")          # โลโก้บริษัท (มุมขวาของหน้า)
    template_logo_path = db.Column(db.String(500), default="")  # โลโก้ลูกค้า/template (มุมซ้าย)
    defaults_json = db.Column(db.Text, default="{}")            # ค่าเริ่มต้นฟอร์มของบริษัท
    created_at = db.Column(db.DateTime, default=now)

    @property
    def defaults(self):
        return JsonMixin._load(self.defaults_json, {})


ROLES = {
    "superadmin": "Super Admin (ดูทุกบริษัท)",
    "admin": "Admin บริษัท (จัดการผู้ใช้ / อนุมัติ)",
    "editor": "ผู้จัดทำเอกสาร",
    "viewer": "ดูอย่างเดียว",
}


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey("company.id"), nullable=True)
    email = db.Column(db.String(200), unique=True, nullable=False)
    name = db.Column(db.String(200), nullable=False)
    role = db.Column(db.String(20), nullable=False, default="editor")
    password_hash = db.Column(db.String(300), nullable=False)
    active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=now)
    last_login = db.Column(db.DateTime)
    signature_path = db.Column(db.String(500))
    position = db.Column(db.String(200))
    company = db.relationship("Company")

    def set_password(self, pw):
        self.password_hash = generate_password_hash(pw)

    def check_password(self, pw):
        return check_password_hash(self.password_hash, pw)

    @property
    def is_active(self):
        return bool(self.active)

    @property
    def is_super(self):
        return self.role == "superadmin"

    def can_edit(self, job=None):
        if self.role not in ("superadmin", "admin", "editor"):
            return False
        return job is None or self.is_super or job.company_id == self.company_id

    def can_approve(self, job):
        return self.is_super or (self.role == "admin" and job.company_id == self.company_id)

    def can_view(self, job):
        return self.is_super or job.company_id == self.company_id


STATUS = {
    "draft": ("ร่าง", "s-draft"),
    "ready": ("พร้อมส่งตรวจ", "s-ready"),
    "submitted": ("รอตรวจ", "s-review"),
    "rejected": ("ถูกตีกลับ", "s-reject"),
    "approved": ("อนุมัติแล้ว", "s-ok"),
}


class Job(db.Model, JsonMixin):
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey("company.id"), nullable=False, index=True)
    link_name = db.Column(db.String(300), nullable=False)
    status = db.Column(db.String(20), default="draft", index=True)
    fields_json = db.Column(db.Text, default="{}")
    closures_json = db.Column(db.Text, default="[]")
    assign_json = db.Column(db.Text, default="{}")
    conf_json = db.Column(db.Text, default="{}")
    method_json = db.Column(db.Text, default="{}")
    review_json = db.Column(db.Text, default="[]")
    sheet_cfg_json = db.Column(db.Text, default="{}")
    kmz_json = db.Column(db.Text, default="[]")
    reject_comment = db.Column(db.Text, default="")
    created_by = db.Column(db.Integer, db.ForeignKey("user.id"))
    updated_by = db.Column(db.Integer, db.ForeignKey("user.id"))
    created_at = db.Column(db.DateTime, default=now)
    updated_at = db.Column(db.DateTime, default=now, onupdate=now)
    company = db.relationship("Company")
    creator = db.relationship("User", foreign_keys=[created_by])
    updater = db.relationship("User", foreign_keys=[updated_by])

    def j(self, name, default):
        return self._load(getattr(self, name + "_json"), default)

    def setj(self, name, value):
        setattr(self, name + "_json", json.dumps(value, ensure_ascii=False))

    @property
    def locked(self):
        return self.status in ("submitted", "approved")

    @property
    def fields(self):
        return self.j("fields", {})

    @property
    def assign(self):
        return self.j("assign", {})

    @property
    def closures(self):
        return self.j("closures", [])


class Photo(db.Model):
    id = db.Column(db.String(32), primary_key=True)
    job_id = db.Column(db.Integer, db.ForeignKey("job.id"), index=True, nullable=False)
    rel = db.Column(db.String(500))
    name = db.Column(db.String(300))
    group = db.Column(db.String(30))
    sub = db.Column(db.String(30))
    path = db.Column(db.String(500))
    thumb = db.Column(db.String(500))
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    blurry = db.Column(db.Boolean, default=False)
    kind = db.Column(db.String(30))      # ประเภทรูป (จาก AI / จากการจัดของคน)
    kconf = db.Column(db.Float)
    uploaded_by = db.Column(db.Integer, db.ForeignKey("user.id"))
    created_at = db.Column(db.DateTime, default=now)


class Attachment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.Integer, db.ForeignKey("job.id"), index=True, nullable=False)
    kind = db.Column(db.String(20))  # sheet | asbuilt | otdr
    filename = db.Column(db.String(300))
    path = db.Column(db.String(500))
    pages = db.Column(db.Integer)
    meta_json = db.Column(db.Text)       # ผลอ่านไฟล์ .sor
    uploaded_by = db.Column(db.Integer, db.ForeignKey("user.id"))
    created_at = db.Column(db.DateTime, default=now)
    uploader = db.relationship("User")

    @property
    def meta(self):
        return JsonMixin._load(self.meta_json, {})


class Document(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.Integer, db.ForeignKey("job.id"), index=True, nullable=False)
    kind = db.Column(db.String(20))  # PAT | ASBUILT
    version = db.Column(db.Integer)
    filename = db.Column(db.String(300))
    path = db.Column(db.String(500))
    pages = db.Column(db.Integer)
    size = db.Column(db.Integer)
    created_by = db.Column(db.Integer, db.ForeignKey("user.id"))
    created_at = db.Column(db.DateTime, default=now)
    creator = db.relationship("User")


class Audit(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, index=True)
    job_id = db.Column(db.Integer, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    action = db.Column(db.String(50))
    detail = db.Column(db.Text, default="")
    ip = db.Column(db.String(64), default="")
    created_at = db.Column(db.DateTime, default=now, index=True)
    user = db.relationship("User")


class Example(db.Model):
    """รูปตัวอย่างที่ใช้สอน AI (มาจากการที่คนจัด/ยืนยันรูป)"""
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, index=True)
    kind = db.Column(db.String(30), index=True)
    path = db.Column(db.String(500))
    photo_id = db.Column(db.String(32))
    created_at = db.Column(db.DateTime, default=now)


MIGRATIONS = [  # (table, column, DDL type) — เพิ่มคอลัมน์ใหม่ให้ฐานข้อมูลเดิมอัตโนมัติ
    ("photo", "kind", "VARCHAR(30)"), ("photo", "kconf", "FLOAT"),
    ("attachment", "meta_json", "TEXT"),
    ("user", "signature_path", "VARCHAR(500)"), ("user", "position", "VARCHAR(200)"),
]


def auto_migrate():
    from sqlalchemy import inspect, text
    insp = inspect(db.engine)
    for table, col, typ in MIGRATIONS:
        if table in insp.get_table_names() and col not in [c["name"] for c in insp.get_columns(table)]:
            q = '"user"' if table == "user" else table
            with db.engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE {q} ADD COLUMN {col} {typ}"))
