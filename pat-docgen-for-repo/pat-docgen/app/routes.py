import io
import os
import re
import shutil
import uuid
from collections import Counter

from flask import (Blueprint, abort, current_app, flash, jsonify, redirect, render_template, request,
                   send_file, url_for)
from flask_login import current_user, login_required, login_user, logout_user
from sqlalchemy import func, or_

from . import pdfgen
from .importer import REVIEW_BELOW, run_import
from .layout import DEFAULT_FIELDS, all_slot_keys, build_pages, closure_list, link_budget, sites_from_link
from .models import ROLES, STATUS, Attachment, Audit, Company, Document, Job, Photo, User, db, now

bp = Blueprint("main", __name__)

# ---------------------------------------------------------------- helpers


def log(action, detail="", job=None, company_id=None):
    db.session.add(Audit(company_id=company_id or (job.company_id if job else (current_user.company_id if current_user.is_authenticated else None)),
                         job_id=job.id if job else None,
                         user_id=current_user.id if current_user.is_authenticated else None,
                         action=action, detail=detail[:2000], ip=request.headers.get("X-Forwarded-For", request.remote_addr or "")[:64]))


def job_dir(job):
    d = os.path.join(current_app.config["DATA_DIR"], "jobs", str(job.id))
    os.makedirs(d, exist_ok=True)
    return d


def get_job(job_id, edit=False):
    job = db.session.get(Job, job_id)
    if not job or not current_user.can_view(job):
        abort(404)
    if edit and (not current_user.can_edit(job) or job.locked):
        abort(403, "งานนี้ส่งตรวจ/อนุมัติแล้ว — กด 'เปิดแก้ไขใหม่' ก่อนแก้")
    return job


def scoped_jobs():
    q = Job.query
    if not current_user.is_super:
        q = q.filter(Job.company_id == current_user.company_id)
    return q


def job_pages(job):
    return build_pages(job.link_name, {**DEFAULT_FIELDS, **job.fields}, closure_list(job.fields, job.closures))


def progress(job, pages=None):
    pages = pages or job_pages(job)
    keys = all_slot_keys(pages)
    a = job.assign
    filled = sum(1 for k in keys if a.get(k))
    review = [k for k in job.j("review", []) if k in keys and a.get(k)]
    return {"filled": filled, "total": len(keys), "review": len(review)}


def admin_required():
    if current_user.role not in ("superadmin", "admin"):
        abort(403)

# ---------------------------------------------------------------- auth


@bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        u = User.query.filter(func.lower(User.email) == request.form.get("email", "").strip().lower()).first()
        if u and u.active and u.check_password(request.form.get("password", "")):
            login_user(u, remember=True)
            u.last_login = now()
            log("login", "เข้าสู่ระบบ", company_id=u.company_id)
            db.session.commit()
            return redirect(request.args.get("next") or url_for("main.dashboard"))
        flash("อีเมลหรือรหัสผ่านไม่ถูกต้อง", "error")
    return render_template("login.html")


@bp.route("/logout", methods=["POST"])
@login_required
def logout():
    log("logout", "ออกจากระบบ")
    db.session.commit()
    logout_user()
    return redirect(url_for("main.login"))

# ---------------------------------------------------------------- dashboard


@bp.route("/")
@login_required
def dashboard():
    q = scoped_jobs()
    st = request.args.get("status", "")
    term = request.args.get("q", "").strip()
    cid = request.args.get("company", type=int)
    if current_user.is_super and cid:
        q = q.filter(Job.company_id == cid)
    counts = Counter(s for (s,) in q.with_entities(Job.status).all())
    if st:
        q = q.filter(Job.status == st)
    if term:
        q = q.filter(or_(Job.link_name.ilike(f"%{term}%"), Job.fields_json.ilike(f"%{term}%")))
    jobs = q.order_by(Job.updated_at.desc()).limit(300).all()
    rows = [(j, progress(j)) for j in jobs]
    by_user = (scoped_jobs().join(User, Job.created_by == User.id)
               .with_entities(User.name, func.count(Job.id)).group_by(User.name)
               .order_by(func.count(Job.id).desc()).limit(8).all())
    aq = Audit.query
    if not current_user.is_super:
        aq = aq.filter(Audit.company_id == current_user.company_id)
    recent = aq.filter(~Audit.action.in_(["login", "logout", "download"])).order_by(Audit.created_at.desc()).limit(8).all()
    companies = Company.query.order_by(Company.name).all() if current_user.is_super else []
    return render_template("dashboard.html", rows=rows, counts=counts, total=sum(counts.values()), st=st, term=term,
                           STATUS=STATUS, by_user=by_user, recent=recent, companies=companies, cid=cid)

# ---------------------------------------------------------------- job create / edit


FIELD_FORM = [
    ("ข้อมูลหน้าปก", [("sap_no", "SAP No."), ("project", "Project"), ("region", "Region"), ("activity", "Activity")]),
    ("หน้ารูป", [("equip1", "อุปกรณ์ Site 1 (เช่น CX600)"), ("equip2", "อุปกรณ์ Site 2 (เช่น CPE)"),
                 ("power_core1", "Test Power Core (1)"), ("power_core2", "Test Power Core (2)"),
                 ("closures", "ลำดับหัวต่อ (ว่าง = อ่านจาก KMZ)")]),
    ("Link Budget", [("lb_L", "L ความยาว (Km.) จาก OTDR"), ("lb_aF", "aF (dB/Km.)"), ("lb_NE", "NE จำนวนจุด splice"),
                     ("lb_aE", "aE (dB)"), ("lb_NC", "NC จำนวน Connector"), ("lb_aC", "aC (dB)"),
                     ("lb_otdr_max", "OTDR Max (dB)"), ("lb_otdr_min", "OTDR Min (dB)"), ("lb_note", "หมายเหตุ")]),
]


@bp.route("/jobs/new", methods=["GET", "POST"])
@login_required
def job_new():
    if not current_user.can_edit():
        abort(403)
    companies = Company.query.order_by(Company.name).all() if current_user.is_super else []
    if request.method == "POST":
        link = request.form.get("link_name", "").strip()
        if not link:
            flash("กรุณากรอก Link Name", "error")
            return redirect(url_for("main.job_new"))
        cid = request.form.get("company_id", type=int) if current_user.is_super else current_user.company_id
        company = db.session.get(Company, cid) if cid else None
        if not company:
            flash("กรุณาเลือกบริษัท", "error")
            return redirect(url_for("main.job_new"))
        fields = {**DEFAULT_FIELDS, **company.defaults}
        job = Job(company_id=company.id, link_name=link, created_by=current_user.id, updated_by=current_user.id)
        job.setj("fields", fields)
        db.session.add(job)
        db.session.flush()
        log("job_create", f"สร้างงาน {link}", job)
        db.session.commit()
        z = request.files.get("zip")
        if z and z.filename:
            do_import(job, z.read(), z.filename)
        return redirect(url_for("main.job_view", job_id=job.id))
    return render_template("job_new.html", companies=companies)


@bp.route("/jobs/<int:job_id>")
@login_required
def job_view(job_id):
    job = get_job(job_id)
    pages = job_pages(job)
    prog = progress(job, pages)
    f = {**DEFAULT_FIELDS, **job.fields}
    docs = Document.query.filter_by(job_id=job.id).order_by(Document.created_at.desc()).all()
    atts = Attachment.query.filter_by(job_id=job.id).order_by(Attachment.created_at).all()
    hist = Audit.query.filter_by(job_id=job.id).order_by(Audit.created_at.desc()).limit(60).all()
    s1, s2 = sites_from_link(job.link_name)
    checks = run_checks(job, pages, f, atts)
    return render_template("job.html", job=job, f=f, FIELD_FORM=FIELD_FORM, prog=prog, docs=docs, atts=atts, hist=hist,
                           STATUS=STATUS, s1=s1, s2=s2, lb=link_budget(f), checks=checks,
                           sheet_cfg=job.j("sheet_cfg", {}), kmz=job.j("kmz", []), pages=pages)


def run_checks(job, pages, f, atts):
    out = []
    a = job.assign
    missing = [s for p in pages for s in p["slots"] if s["caption"] and not a.get(s["key"])]
    if missing:
        out.append(("warn", f"ยังไม่มีรูป {len(missing)} ช่อง (จะแสดง 'ไม่มีรูป' ใน PDF)"))
    else:
        out.append(("ok", "รูปครบทุกช่อง"))
    rv = [k for k in job.j("review", []) if a.get(k)]
    if rv:
        out.append(("warn", f"มี {len(rv)} ช่องที่ระบบไม่มั่นใจ — ควรตรวจในหน้า 'จัดรูป'"))
    if not any(x.kind == "sheet" for x in atts):
        out.append(("warn", "ยังไม่ได้แนบ PDF ฟอร์มจาก Google Sheet (Check list / Loss / Material) — PDF จะไม่มีหน้าเหล่านี้"))
    else:
        out.append(("ok", "แนบ PDF ฟอร์มจาก Google Sheet แล้ว"))
    if not job.closures and not f.get("closures"):
        out.append(("warn", "ไม่พบรายชื่อหัวต่อ (ไม่มี KMZ และไม่มีโฟลเดอร์ SJ#/BJ#)"))
    lb = link_budget(f)
    try:
        ne = float(f.get("lb_NE") or "nan")
        if job.closures and ne == ne and int(ne) != len(closure_list(f, job.closures)):
            out.append(("warn", f"NE ใน Link Budget = {int(ne)} แต่จำนวนหัวต่อ = {len(closure_list(f, job.closures))}"))
    except ValueError:
        pass
    if lb["total"] is not None and lb["otdr_max"] is not None and abs(lb["otdr_max"] - lb["total"]) > 1.5 and not f.get("lb_note"):
        out.append(("warn", f"OTDR Max {lb['otdr_max']:.2f} dB ต่างจากค่าคำนวณ {lb['total']:.2f} dB เกิน 1.5 dB — ควรใส่หมายเหตุ"))
    if not any(x.kind in ("asbuilt", "otdr") for x in atts):
        out.append(("info", "ยังไม่ได้แนบแบบ As-Built / Report OTDR — AS-BUILT จะเท่ากับ PAT"))
    blurry = Photo.query.filter_by(job_id=job.id, blurry=True).count()
    if blurry:
        out.append(("warn", f"รูปอาจเบลอ {blurry} รูป"))
    return out


@bp.route("/jobs/<int:job_id>/fields", methods=["POST"])
@login_required
def job_fields(job_id):
    job = get_job(job_id, edit=True)
    f = job.fields
    changed = []
    link = request.form.get("link_name", "").strip()
    if link and link != job.link_name:
        changed.append("Link Name")
        job.link_name = link
    for _, items in FIELD_FORM:
        for k, _lbl in items:
            if k in request.form:
                v = request.form.get(k, "").strip()
                if f.get(k, "") != v:
                    changed.append(k)
                    f[k] = v
    job.setj("fields", f)
    job.updated_by = current_user.id
    if changed:
        log("fields", "แก้ข้อมูล: " + ", ".join(changed), job)
    db.session.commit()
    flash("บันทึกข้อมูลแล้ว", "ok")
    return redirect(url_for("main.job_view", job_id=job.id) + "#data")

# ---------------------------------------------------------------- import photos


def do_import(job, data, filename):
    s1, s2 = sites_from_link(job.link_name)
    tmpdir = os.path.join(job_dir(job), "photos")
    try:
        res = run_import(data, tmpdir, s1, s2)
    except Exception as e:  # pragma: no cover
        flash(f"Import ไม่สำเร็จ: {e}", "error")
        return
    for p in res["photos"]:
        db.session.add(Photo(id=p["id"], job_id=job.id, rel=p["rel"], name=p["name"], group=p["group"], sub=str(p["sub"] or ""),
                             path=p["path"], thumb=p["thumb"], lat=p.get("lat"), lon=p.get("lon"),
                             blurry=bool(p.get("blurry")), uploaded_by=current_user.id))
    assign, conf, method = job.assign, job.j("conf", {}), job.j("method", {})
    review = set(job.j("review", []))
    added = 0
    for slot, pid in res["assign"].items():
        if not assign.get(slot):
            assign[slot] = pid
            conf[slot] = res["conf"][slot]
            method[slot] = res["method"][slot]
            if res["conf"][slot] < REVIEW_BELOW:
                review.add(slot)
            added += 1
    job.setj("assign", assign)
    job.setj("conf", conf)
    job.setj("method", method)
    job.setj("review", sorted(review))
    cl = job.closures
    for c in res["closures"]:
        if c not in cl:
            cl.append(c)
    job.setj("closures", cl)
    if res["kmz"]:
        job.setj("kmz", res["kmz"])
        f = job.fields
        if not f.get("lb_NE"):
            f["lb_NE"] = str(len([n for n, _, _ in res["kmz"] if re.search(r"[SB]J", n, re.I)]))
            job.setj("fields", f)
    job.updated_by = current_user.id
    unknown = sum(1 for p in res["photos"] if p["group"] == "unknown")
    log("import", f"Import {filename}: {len(res['photos'])} รูป, วางเข้าช่องอัตโนมัติ {added} ช่อง, "
                  f"หัวต่อ {len(res['closures'])} จุด, ไม่ทราบกลุ่ม {unknown} รูป" + (", มี KMZ" if res["kmz"] else ""), job)
    db.session.commit()
    flash(f"Import {len(res['photos'])} รูป — วางเข้าช่องให้ {added} ช่อง (ให้ตรวจ {len([s for s in review if assign.get(s)])} ช่อง)", "ok")


@bp.route("/jobs/<int:job_id>/import", methods=["POST"])
@login_required
def job_import(job_id):
    job = get_job(job_id, edit=True)
    files = [f for f in request.files.getlist("zip") if f and f.filename]
    if not files:
        flash("กรุณาเลือกไฟล์ ZIP", "error")
        return redirect(url_for("main.job_view", job_id=job.id))
    if len(files) == 1 and files[0].filename.lower().endswith(".zip"):
        do_import(job, files[0].read(), files[0].filename)
    else:
        # เลือกหลายไฟล์ / ทั้งโฟลเดอร์จาก browser -> ห่อเป็น zip ในหน่วยความจำ (เก็บชื่อโฟลเดอร์ไว้)
        import zipfile
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            for f in files:
                z.writestr(f.filename, f.read())
        do_import(job, buf.getvalue(), f"{len(files)} ไฟล์")
    return redirect(url_for("main.job_photos", job_id=job.id))


@bp.route("/jobs/<int:job_id>/photos")
@login_required
def job_photos(job_id):
    job = get_job(job_id)
    pages = job_pages(job)
    photos = Photo.query.filter_by(job_id=job.id).order_by(Photo.rel).all()
    used = Counter(v for v in job.assign.values() if v and v != "NA")
    groups = {}
    for p in photos:
        label = {"site": "Site", "closure": "หัวต่อ", "plug": "Plug Port", "power": "Power", "planwork": "Planwork",
                 "design": "Design", "route": "Route", "unknown": "ไม่ทราบกลุ่ม"}.get(p.group, p.group)
        key = os.path.dirname(p.rel) or "(root)"
        groups.setdefault(key, {"label": label, "items": []})["items"].append(p)
    sheet_n = len(pdfgen.parse_ranges(job.j("sheet_cfg", {}).get("forms", ""), job.j("sheet_cfg", {}).get("count", 0))) or 5
    pno, n = [], 1
    for p in pages:
        pno.append(n)
        n += sheet_n if p["kind"] == "sheet" else 1
    s1, s2 = sites_from_link(job.link_name)
    c1, c2 = s1.split("_", 1)[-1], s2.split("_", 1)[-1]
    def pref(key):
        if key.startswith("site1"): return c1
        if key.startswith("site2"): return c2
        if key.startswith("plug1"): return "PLUG*" + c1
        if key.startswith("plug2"): return "PLUG*" + c2
        if key.startswith("closure:"): return key.split(":")[1]
        if key.startswith("power"): return "POWER"
        if key == "planwork": return "PLAN"
        return ""
    prefs = {k: pref(k) for k in all_slot_keys(pages)}
    return render_template("photos.html", job=job, pages=pages, pno=pno, prefs=prefs, photos=photos, used=used, groups=groups,
                           assign=job.assign, conf=job.j("conf", {}), method=job.j("method", {}),
                           review=set(job.j("review", [])), prog=progress(job, pages), STATUS=STATUS)


@bp.route("/jobs/<int:job_id>/assign", methods=["POST"])
@login_required
def job_assign(job_id):
    job = get_job(job_id, edit=True)
    slot = request.form.get("slot", "")
    pid = request.form.get("photo", "")
    if slot not in all_slot_keys(job_pages(job)):
        return jsonify(ok=False, error="slot"), 400
    a, review, method, conf = job.assign, set(job.j("review", [])), job.j("method", {}), job.j("conf", {})
    old = a.get(slot)
    if pid == "":
        a.pop(slot, None)
    elif pid == "NA":
        a[slot] = "NA"
    else:
        p = db.session.get(Photo, pid)
        if not p or p.job_id != job.id:
            return jsonify(ok=False, error="photo"), 400
        a[slot] = pid
    method[slot] = "manual"
    conf[slot] = 1.0
    review.discard(slot)
    job.setj("assign", a)
    job.setj("review", sorted(review))
    job.setj("method", method)
    job.setj("conf", conf)
    job.updated_by = current_user.id
    log("assign", f"{slot}: {old or '-'} → {pid or 'ว่าง'}", job)
    db.session.commit()
    return jsonify(ok=True, prog=progress(job))


@bp.route("/jobs/<int:job_id>/confirm-all", methods=["POST"])
@login_required
def job_confirm_all(job_id):
    job = get_job(job_id, edit=True)
    n = len(job.j("review", []))
    job.setj("review", [])
    log("confirm", f"ยืนยันรูปที่ระบบเลือกทั้งหมด ({n} ช่อง)", job)
    db.session.commit()
    flash("ยืนยันทุกช่องแล้ว", "ok")
    return redirect(url_for("main.job_photos", job_id=job.id))


@bp.route("/photo/<pid>/<size>")
@login_required
def photo_file(pid, size):
    p = db.session.get(Photo, pid)
    if not p:
        abort(404)
    get_job(p.job_id)
    return send_file(p.thumb if size == "t" else p.path, mimetype="image/jpeg", max_age=86400)

# ---------------------------------------------------------------- attachments


ATT_KIND = {"sheet": "PDF ฟอร์มจาก Google Sheet", "asbuilt": "แบบ As-Built (ปก/Symbol/Core Assignment)", "otdr": "Report OTDR"}


@bp.route("/jobs/<int:job_id>/attach", methods=["POST"])
@login_required
def job_attach(job_id):
    job = get_job(job_id, edit=True)
    kind = request.form.get("kind")
    if kind not in ATT_KIND:
        abort(400)
    for f in request.files.getlist("file"):
        if not f or not f.filename:
            continue
        data = f.read()
        if not data.startswith(b"%PDF"):
            flash(f"{f.filename} ไม่ใช่ไฟล์ PDF", "error")
            continue
        d = os.path.join(job_dir(job), "files")
        os.makedirs(d, exist_ok=True)
        path = os.path.join(d, uuid.uuid4().hex[:10] + ".pdf")
        with open(path, "wb") as fh:
            fh.write(data)
        if kind == "sheet":
            for old in Attachment.query.filter_by(job_id=job.id, kind="sheet").all():
                db.session.delete(old)
            cfg = pdfgen.detect_sheet_pages(data)
            job.setj("sheet_cfg", cfg)
            msg = (f"ตรวจพบหน้าฟอร์ม {cfg['forms'] or '-'}, Link budget {cfg['linkbudget'] or '-'}, "
                   f"Deficiency {cfg['deficiency'] or '-'} (จาก {cfg['count']} หน้า)")
            flash(msg, "ok")
        att = Attachment(job_id=job.id, kind=kind, filename=f.filename, path=path, pages=pdfgen.count_pages(data),
                         uploaded_by=current_user.id)
        db.session.add(att)
        log("attach", f"แนบ {ATT_KIND[kind]}: {f.filename}", job)
    job.updated_by = current_user.id
    db.session.commit()
    return redirect(url_for("main.job_view", job_id=job.id) + "#files")


@bp.route("/jobs/<int:job_id>/sheet-cfg", methods=["POST"])
@login_required
def job_sheet_cfg(job_id):
    job = get_job(job_id, edit=True)
    cfg = job.j("sheet_cfg", {})
    for k in ("forms", "linkbudget", "deficiency"):
        cfg[k] = request.form.get(k, "").strip()
    job.setj("sheet_cfg", cfg)
    log("sheet_cfg", f"ตั้งหน้าที่ใช้จาก Google Sheet: ฟอร์ม {cfg['forms']}, LB {cfg['linkbudget']}, Deficiency {cfg['deficiency']}", job)
    db.session.commit()
    flash("บันทึกการเลือกหน้าแล้ว", "ok")
    return redirect(url_for("main.job_view", job_id=job.id) + "#files")


@bp.route("/attach/<int:att_id>/delete", methods=["POST"])
@login_required
def att_delete(att_id):
    att = db.session.get(Attachment, att_id) or abort(404)
    job = get_job(att.job_id, edit=True)
    log("detach", f"ลบไฟล์แนบ {att.filename}", job)
    db.session.delete(att)
    db.session.commit()
    return redirect(url_for("main.job_view", job_id=job.id) + "#files")

# ---------------------------------------------------------------- generate


def safe_name(s):
    return re.sub(r"[^\w\-.]+", "_", s).strip("_")


@bp.route("/jobs/<int:job_id>/generate", methods=["POST"])
@login_required
def job_generate(job_id):
    job = get_job(job_id, edit=True)
    pages = job_pages(job)
    photos = {p.id: p.path for p in Photo.query.filter_by(job_id=job.id).all()}
    by_slot = {}
    for slot, pid in job.assign.items():
        if pid == "NA":
            by_slot[slot] = "NA"
        elif pid in photos:
            by_slot[slot] = photos[pid]
    company = job.company
    comp = {"name": company.name, "name_th": company.name_th, "address": company.address}
    atts = Attachment.query.filter_by(job_id=job.id).order_by(Attachment.created_at).all()
    sheet = next((a for a in atts if a.kind == "sheet"), None)
    sheet_bytes = open(sheet.path, "rb").read() if sheet else None
    jobd = {"link_name": job.link_name, "fields": {**DEFAULT_FIELDS, **job.fields}, "template_version": "3.0"}
    try:
        pat = pdfgen.build_pat(jobd, comp, pages, by_slot, company.template_logo_path or None, company.logo_path or None,
                               sheet_bytes, job.j("sheet_cfg", {}))
        extras = [open(a.path, "rb").read() for a in atts if a.kind == "asbuilt"] + \
                 [open(a.path, "rb").read() for a in atts if a.kind == "otdr"]
        asb = pdfgen.build_asbuilt(pat, extras)
    except Exception as e:
        current_app.logger.exception("generate failed")
        flash(f"สร้าง PDF ไม่สำเร็จ: {e}", "error")
        return redirect(url_for("main.job_view", job_id=job.id))
    ver = (db.session.query(func.max(Document.version)).filter_by(job_id=job.id).scalar() or 0) + 1
    d = os.path.join(job_dir(job), "out")
    os.makedirs(d, exist_ok=True)
    base = safe_name(job.link_name.replace(" ", "_"))
    for kind, data, prefix in (("PAT", pat, "PAT_"), ("ASBUILT", asb, "PATAS-BUILT_")):
        fn = f"{prefix}{base}.pdf"
        path = os.path.join(d, f"v{ver}_{fn}")
        with open(path, "wb") as fh:
            fh.write(data)
        db.session.add(Document(job_id=job.id, kind=kind, version=ver, filename=fn, path=path,
                                pages=pdfgen.count_pages(data), size=len(data), created_by=current_user.id))
    if job.status in ("draft", "rejected"):
        job.status = "ready"
    job.updated_by = current_user.id
    log("generate", f"สร้าง PDF v{ver}: PAT {pdfgen.count_pages(pat)} หน้า, AS-BUILT {pdfgen.count_pages(asb)} หน้า", job)
    db.session.commit()
    flash(f"สร้าง PDF v{ver} เรียบร้อย", "ok")
    return redirect(url_for("main.job_view", job_id=job.id) + "#docs")


@bp.route("/docs/<int:doc_id>")
@login_required
def doc_download(doc_id):
    d = db.session.get(Document, doc_id) or abort(404)
    job = get_job(d.job_id)
    log("download", f"ดาวน์โหลด {d.kind} v{d.version}", job)
    db.session.commit()
    return send_file(d.path, as_attachment=request.args.get("inline") != "1", download_name=d.filename,
                     mimetype="application/pdf")

# ---------------------------------------------------------------- workflow


@bp.route("/jobs/<int:job_id>/status", methods=["POST"])
@login_required
def job_status(job_id):
    job = get_job(job_id)
    action = request.form.get("action")
    comment = request.form.get("comment", "").strip()
    if action == "submit" and current_user.can_edit(job):
        if not Document.query.filter_by(job_id=job.id).count():
            flash("กรุณาสร้าง PDF ก่อนส่งตรวจ", "error")
            return redirect(url_for("main.job_view", job_id=job.id))
        job.status = "submitted"
        log("submit", "ส่งตรวจ" + (f": {comment}" if comment else ""), job)
    elif action in ("approve", "reject") and current_user.can_approve(job):
        job.status = "approved" if action == "approve" else "rejected"
        job.reject_comment = comment if action == "reject" else ""
        log(action, ("อนุมัติ" if action == "approve" else "ตีกลับ") + (f": {comment}" if comment else ""), job)
    elif action == "reopen" and current_user.can_edit(job):
        job.status = "draft"
        log("reopen", "เปิดแก้ไขใหม่", job)
    elif action == "delete" and current_user.can_approve(job):
        log("delete", f"ลบงาน {job.link_name}", job)
        Photo.query.filter_by(job_id=job.id).delete()
        Attachment.query.filter_by(job_id=job.id).delete()
        Document.query.filter_by(job_id=job.id).delete()
        shutil.rmtree(job_dir(job), ignore_errors=True)
        db.session.delete(job)
        db.session.commit()
        flash("ลบงานแล้ว", "ok")
        return redirect(url_for("main.dashboard"))
    else:
        abort(403)
    job.updated_by = current_user.id
    db.session.commit()
    return redirect(url_for("main.job_view", job_id=job.id))

# ---------------------------------------------------------------- admin


@bp.route("/users", methods=["GET", "POST"])
@login_required
def users():
    admin_required()
    if request.method == "POST":
        act = request.form.get("act")
        if act == "add":
            email = request.form.get("email", "").strip().lower()
            role = request.form.get("role", "editor")
            if role == "superadmin" and not current_user.is_super:
                abort(403)
            cid = request.form.get("company_id", type=int) if current_user.is_super else current_user.company_id
            if role != "superadmin" and not cid:
                flash("กรุณาเลือกบริษัท", "error")
            elif not email or User.query.filter(func.lower(User.email) == email).first():
                flash("อีเมลซ้ำหรือว่าง", "error")
            else:
                u = User(email=email, name=request.form.get("name", "").strip() or email, role=role,
                         company_id=None if role == "superadmin" else cid)
                u.set_password(request.form.get("password") or "changeme123")
                db.session.add(u)
                log("user_add", f"เพิ่มผู้ใช้ {email} ({ROLES.get(role)})", company_id=cid)
                flash("เพิ่มผู้ใช้แล้ว", "ok")
        else:
            u = db.session.get(User, request.form.get("uid", type=int)) or abort(404)
            if not current_user.is_super and u.company_id != current_user.company_id:
                abort(403)
            if act == "toggle" and u.id != current_user.id:
                u.active = not u.active
                log("user_toggle", f"{'เปิด' if u.active else 'ปิด'}การใช้งาน {u.email}", company_id=u.company_id)
            elif act == "reset":
                pw = request.form.get("password") or "changeme123"
                u.set_password(pw)
                log("user_reset", f"ตั้งรหัสผ่านใหม่ {u.email}", company_id=u.company_id)
                flash(f"ตั้งรหัสผ่านใหม่ให้ {u.email} แล้ว", "ok")
        db.session.commit()
        return redirect(url_for("main.users"))
    q = User.query
    if not current_user.is_super:
        q = q.filter(User.company_id == current_user.company_id)
    us = q.order_by(User.company_id, User.name).all()
    companies = Company.query.order_by(Company.name).all() if current_user.is_super else []
    return render_template("users.html", users=us, ROLES=ROLES, companies=companies)


@bp.route("/companies", methods=["GET", "POST"])
@login_required
def companies():
    admin_required()
    if request.method == "POST":
        cid = request.form.get("cid", type=int)
        if cid:
            c = db.session.get(Company, cid) or abort(404)
            if not current_user.is_super and c.id != current_user.company_id:
                abort(403)
        else:
            if not current_user.is_super:
                abort(403)
            name = request.form.get("name", "").strip()
            if not name or Company.query.filter_by(name=name).first():
                flash("ชื่อบริษัทว่างหรือซ้ำ", "error")
                return redirect(url_for("main.companies"))
            c = Company(name=name)
            db.session.add(c)
            db.session.flush()
        c.name = request.form.get("name", c.name).strip() or c.name
        c.name_th = request.form.get("name_th", "").strip()
        c.address = request.form.get("address", "").strip()
        defaults = c.defaults
        for k in ("project", "region", "activity", "equip1", "equip2"):
            if request.form.get("d_" + k) is not None:
                defaults[k] = request.form.get("d_" + k).strip()
        c.defaults_json = __import__("json").dumps(defaults, ensure_ascii=False)
        d = os.path.join(current_app.config["DATA_DIR"], "logos")
        os.makedirs(d, exist_ok=True)
        for field, attr in (("logo", "logo_path"), ("template_logo", "template_logo_path")):
            f = request.files.get(field)
            if f and f.filename:
                ext = os.path.splitext(f.filename)[1].lower()
                if ext not in (".png", ".jpg", ".jpeg"):
                    flash("โลโก้ต้องเป็น PNG/JPG", "error")
                    continue
                p = os.path.join(d, f"c{c.id}_{field}_{uuid.uuid4().hex[:6]}{ext}")
                f.save(p)
                setattr(c, attr, p)
        log("company", f"บันทึกข้อมูลบริษัท {c.name}", company_id=c.id)
        db.session.commit()
        flash("บันทึกแล้ว", "ok")
        return redirect(url_for("main.companies"))
    cs = Company.query.order_by(Company.name).all() if current_user.is_super else [current_user.company]
    return render_template("companies.html", companies=cs)


@bp.route("/logo/<int:cid>/<which>")
@login_required
def logo(cid, which):
    c = db.session.get(Company, cid) or abort(404)
    p = c.logo_path if which == "company" else c.template_logo_path
    if not p or not os.path.exists(p):
        abort(404)
    return send_file(p)


@bp.route("/audit")
@login_required
def audit():
    q = Audit.query
    if not current_user.is_super:
        if current_user.role != "admin":
            q = q.filter(Audit.user_id == current_user.id)
        else:
            q = q.filter(Audit.company_id == current_user.company_id)
    who = request.args.get("user", type=int)
    if who:
        q = q.filter(Audit.user_id == who)
    rows = q.order_by(Audit.created_at.desc()).limit(500).all()
    jobs = {j.id: j for j in Job.query.filter(Job.id.in_({r.job_id for r in rows if r.job_id})).all()}
    return render_template("audit.html", rows=rows, jobs=jobs)
