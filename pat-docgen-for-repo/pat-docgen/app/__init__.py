import os
import secrets

from flask import Flask, abort, request, session
from flask_login import LoginManager

from .models import Company, User, db

login_manager = LoginManager()
login_manager.login_view = "main.login"
login_manager.login_message = "กรุณาเข้าสู่ระบบ"


@login_manager.user_loader
def load_user(uid):
    return db.session.get(User, int(uid))


def create_app(test_config=None):
    app = Flask(__name__)
    data_dir = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data"))
    os.makedirs(data_dir, exist_ok=True)
    db_url = os.environ.get("DATABASE_URL", "sqlite:///" + os.path.join(data_dir, "app.db"))
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    app.config.update(
        SECRET_KEY=os.environ.get("SECRET_KEY") or _persistent_secret(data_dir),
        SQLALCHEMY_DATABASE_URI=db_url,
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        MAX_CONTENT_LENGTH=int(os.environ.get("MAX_UPLOAD_MB", "800")) * 1024 * 1024,
        DATA_DIR=data_dir,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
    )
    if test_config:
        app.config.update(test_config)

    db.init_app(app)
    login_manager.init_app(app)

    @app.before_request
    def csrf_protect():
        if request.method == "POST":
            token = session.get("_csrf")
            sent = request.form.get("_csrf") or request.headers.get("X-CSRF")
            if not token or token != sent:
                abort(400, "CSRF token ไม่ถูกต้อง กรุณารีเฟรชหน้าแล้วลองใหม่")

    @app.context_processor
    def inject_csrf():
        if "_csrf" not in session:
            session["_csrf"] = secrets.token_urlsafe(24)
        return {"csrf": session["_csrf"]}

    from datetime import timedelta

    @app.template_filter("th")
    def th_time(dt):
        if not dt:
            return ""
        return (dt + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M")

    from .routes import bp
    app.register_blueprint(bp)

    with app.app_context():
        db.create_all()
        seed(app)
    return app


def _persistent_secret(data_dir):
    p = os.path.join(data_dir, ".secret_key")
    if not os.path.exists(p):
        with open(p, "w") as fh:
            fh.write(secrets.token_hex(32))
    return open(p).read().strip()


def seed(app):
    if User.query.count():
        return
    email = os.environ.get("ADMIN_EMAIL", "admin@bbtec.local")
    pw = os.environ.get("ADMIN_PASSWORD")
    if not pw:
        raise RuntimeError("ต้องตั้ง Variable ADMIN_PASSWORD ก่อนเปิดระบบครั้งแรก")
    su = User(email=email, name="BBTEC Admin", role="superadmin")
    su.set_password(pw)
    db.session.add(su)
    if os.environ.get("SEED_DEMO", "0") == "1":
        # บริษัทตัวอย่างสำหรับทดลอง (ใส่ชื่อ/ที่อยู่/โลโก้จริงในหน้า "บริษัท" หลัง login)
        c = Company(name="Demo Company", name_th="บริษัทตัวอย่าง",
                    defaults_json='{"project":"Rehoming Project","region":"North Region","activity":"Rehome Site Dtac"}')
        db.session.add(c)
        db.session.flush()
        for em, nm, role in (("admin@demo.local", "Admin Demo", "admin"), ("doc@demo.local", "ผู้จัดทำเอกสาร", "editor")):
            u = User(email=em, name=nm, role=role, company_id=c.id)
            u.set_password(os.environ.get("DEMO_PASSWORD", secrets.token_urlsafe(9)))
            db.session.add(u)
    db.session.commit()
