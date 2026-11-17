import io
import json
import os
import re
import shutil
import uuid
from collections import Counter

from flask import (Blueprint, abort, current_app, flash, jsonify, redirect, render_template, request,
                   send_file, url_for)
from flask_login import current_user, login_required, login_user, logout_user
from sqlalchemy import func, or_

from . import pdfgen, present, vision
from .importer import (KIND_TH, REVIEW_BELOW, auto_assign, dhash, hamming, features, fiber_length_from_images, haversine, norm_closure,
                       ocr_latlon, photo_time_key, run_import, slot_kind)
from .sorfile import parse_sor
from .layout import DEFAULT_FIELDS, all_slot_keys, build_pages, closure_list, link_budget, sites_from_link
from .models import ROLES, STATUS, Attachment, Audit, Company, Document, Example, Job, Photo, PhotoMemory, SigLib, User, db, now

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
    recent = present.timeline(aq.filter(~Audit.action.in_(["login", "logout", "download", "kmz"]))
                              .order_by(Audit.created_at.desc()).limit(120).all())[:8]
    rjobs = {j.id: j for j in Job.query.filter(Job.id.in_({r["job_id"] for r in recent if r["job_id"]})).all()}
    companies = Company.query.order_by(Company.name).all() if current_user.is_super else []
    return render_template("dashboard.html", rows=rows, counts=counts, total=sum(counts.values()), st=st, term=term,
                           STATUS=STATUS, by_user=by_user, recent=recent, rjobs=rjobs, companies=companies, cid=cid)

# ---------------------------------------------------------------- job create / edit


FIELD_FORM = [
    ("ข้อมูลทั่วไป", [
        ("sap_no", "SAP No. — ใช้ในหน้าปก รายการเอกสาร และสรุปผลตรวจรับ"), ("or_no", "OR No."),
        ("ofc", "OFC — เช่น 12C FIG.8,60C ARSS"), ("core", "Core"),
        ("project", "Project — หน้าปก"), ("region", "Region"), ("activity", "Activity"),
        ("inspect_project", "Project — หน้าสรุปผลตรวจรับ")]),
    ("หน้ารูป", [("equip1", "อุปกรณ์ Site 1 — เช่น CX600"), ("equip2", "อุปกรณ์ Site 2 — เช่น CPE"),
                 ("power_core1", "Test Power Core (1)"), ("power_core2", "Test Power Core (2)"),
                 ("closures", "ลำดับหัวต่อ — เว้นว่างเพื่ออ่านจาก KMZ / โฟลเดอร์")]),
    ("Link Budget", [("lb_L", "L ความยาว (Km.) — อ่านจากไฟล์ .sor ให้อัตโนมัติ"), ("lb_NE", "NE จำนวนจุด splice — นับจากโฟลเดอร์หัวต่อ"),
                     ("lb_note", "หมายเหตุ"),
                     ("lb_aF", "aF (dB/Km.)"), ("lb_aE", "aE (dB)"), ("lb_NC", "NC จำนวน Connector"), ("lb_aC", "aC (dB)"),
                     ("lb_otdr_max", "OTDR Max (dB) — ค่ากำหนด 4.60"), ("lb_otdr_min", "OTDR Min (dB) — ค่ากำหนด 4.44")]),
    ("หน้า Deficiency", [("def_region", "Region — เช่น UPC1"), ("def_date", "Date — เว้นว่าง = วันที่ลงนาม Manager + 5 วัน"),
                         ("def_subcon", "Sub-Contractor name — เว้นว่าง = ชื่อบริษัท")]),
]
DROPDOWN = {"core": "Core", "project": "Project (หน้าปก)", "activity": "Activity"}   # ช่องที่เป็น Dropdown
SEED_OPTIONS = {"project": ["Rehoming Project"], "activity": ["Rehome Site Dtac"]}


def field_options(company, current):
    """ตัวเลือกของ Dropdown = รายการที่บริษัทตั้งไว้ + ค่าเริ่มต้น + ค่าที่งานนี้ใช้อยู่"""
    saved = (company.defaults.get("options") or {}) if company else {}
    out = {}
    for k in DROPDOWN:
        vals = list(saved.get(k) or [])
        for v in SEED_OPTIONS.get(k, []) + [company.defaults.get(k, "") if company else "", current.get(k, "")]:
            if v and v not in vals:
                vals.append(v)
        out[k] = vals
    return out


ADVANCED = {"inspect_project", "closures", "lb_aF", "lb_aE", "lb_NC", "lb_aC", "lb_otdr_max", "lb_otdr_min", "def_date", "def_subcon"}


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
        fields = {**DEFAULT_FIELDS, **{k: v for k, v in company.defaults.items() if k != "options"}}
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
    hist = present.timeline(Audit.query.filter_by(job_id=job.id).order_by(Audit.created_at.desc()).limit(400).all())
    s1, s2 = sites_from_link(job.link_name)
    checks = [(lvl, msg, present.check_target(msg)) for lvl, msg in run_checks(job, pages, f, atts)]
    return render_template("job.html", job=job, f=f, FIELD_FORM=FIELD_FORM, ADVANCED=ADVANCED, OPTS=field_options(job.company, f), prog=prog, docs=docs, atts=atts, hist=hist,
                           STATUS=STATUS, s1=s1, s2=s2, lb=link_budget(f), checks=checks, SIG_ROLES=SIG_ROLES,
                           sheet_cfg=job.j("sheet_cfg", {}), kmz=job.j("kmz", []), pages=pages,
                           coords=closure_coords(job), ai=vision.enabled(),
                           siglib=SigLib.query.filter_by(company_id=job.company_id).order_by(SigLib.name).all())


def run_checks(job, pages, f, atts):
    out = []
    a = job.assign
    missing = [s for p in pages for s in p["slots"] if s["caption"] and not a.get(s["key"])]
    out.append(("warn", f"ยังไม่มีรูป {len(missing)} ช่อง (จะแสดง 'ไม่มีรูป' ใน PDF)") if missing else ("ok", "รูปครบทุกช่อง"))
    rv = [k for k in job.j("review", []) if a.get(k)]
    if rv:
        out.append(("warn", f"มี {len(rv)} ช่องที่ระบบไม่มั่นใจ — ควรตรวจในหน้า 'จัดรูป'"))
    if not any(x.kind == "sheet" for x in atts):
        out.append(("warn", "ยังไม่ได้แนบ PDF จาก Google Sheet (Loss OTDR / ใบสรุปเคเบิลรื้อถอน / Balance Cable)"))
    else:
        out.append(("ok", "แนบ PDF จาก Google Sheet แล้ว"))
    nsor = sum(1 for x in atts if x.kind == "sor")
    out.append(("ok", f"ไฟล์ .sor {nsor} ไฟล์ → หน้า Report OTDR {nsor} หน้า") if nsor else ("warn", "ยังไม่มีไฟล์ .sor (หน้า Report OTDR จะไม่มี)"))
    for role, label in SIG_ROLES:
        if not f.get(f"sig_{role}_name"):
            out.append(("warn", f"ยังไม่ได้ใส่ชื่อผู้ลงนาม: {label}"))
    if not f.get("lb_L"):
        out.append(("warn", "ยังไม่มีค่า L (ความยาว) ใน Link Budget"))
    cl = closure_list(f, job.closures)
    try:
        ne = int(float(f.get("lb_NE") or "nan"))
        if cl and ne != len(cl):
            out.append(("warn", f"NE ใน Link Budget = {ne} แต่จำนวนหัวต่อ = {len(cl)}"))
    except ValueError:
        pass
    miss_ll = [c["closure"] for c in closure_coords(job) if c["lat"] is None]
    if miss_ll:
        out.append(("warn", f"ยังไม่มีพิกัดจุดติดตั้ง closure: {', '.join(miss_ll)}"))
    if not any(x.kind == "asbuilt" for x in atts):
        out.append(("info", "ยังไม่ได้แนบแบบ As-Built — AS-BUILT จะมีเฉพาะ PAT + Report OTDR"))
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
    # ค่าใหม่ที่พิมพ์ใน Dropdown -> เพิ่มเข้ารายการของบริษัท ให้เลือกได้ในงานถัดไป
    co = job.company
    d = co.defaults
    opts = d.get("options") or {}
    learned = False
    for k in DROPDOWN:
        v = (f.get(k) or "").strip()
        if v and v not in (opts.get(k) or []):
            opts.setdefault(k, []).append(v)
            learned = True
    if learned:
        d["options"] = opts
        co.defaults_json = json.dumps(d, ensure_ascii=False)
    if changed:
        log("fields", "แก้ข้อมูล: " + ", ".join(changed), job)
    db.session.commit()
    flash("บันทึกข้อมูลแล้ว", "ok")
    return redirect(url_for("main.job_view", job_id=job.id) + "#data")

# ---------------------------------------------------------------- signatures


SIG_ROLES = [("sender", "ผู้ส่งเอกสาร (หน้ารายการเอกสาร)"), ("sub", "Sub Contractor"),
             ("insp", "Name of Contractor Inspector"), ("mgr", "Name of Contractor Manager (ใช้ในหน้า Deficiency ด้วย)")]


def _save_sig(fs, folder, stem):
    ext = os.path.splitext(fs.filename)[1].lower()
    if ext not in (".png", ".jpg", ".jpeg"):
        raise ValueError("ลายเซ็นต้องเป็นไฟล์ PNG หรือ JPG")
    os.makedirs(folder, exist_ok=True)
    p = os.path.join(folder, f"{stem}_{uuid.uuid4().hex[:6]}.png")
    from PIL import Image
    im = Image.open(fs.stream).convert("RGBA")
    # ทำพื้นขาวให้โปร่งใส (ลายเซ็นที่ถ่าย/สแกนบนกระดาษขาว)
    px = im.load()
    for y in range(im.height):
        for x in range(im.width):
            r, g, b, a = px[x, y]
            if r > 215 and g > 215 and b > 215:
                px[x, y] = (255, 255, 255, 0)
    bbox = im.getbbox()
    if bbox:
        im = im.crop(bbox)
    im.thumbnail((600, 200))
    im.save(p)
    return p


@bp.route("/jobs/<int:job_id>/signatures", methods=["POST"])
@login_required
def job_signatures(job_id):
    job = get_job(job_id, edit=True)
    f = job.fields
    folder = os.path.join(job_dir(job), "sig")
    done = []
    for role, label in SIG_ROLES:
        for k in ("name", "date", "pos"):
            v = request.form.get(f"sig_{role}_{k}")
            if v is not None:
                f[f"sig_{role}_{k}"] = v.strip()
        up = request.files.get(f"sig_{role}_file")
        try:
            if up and up.filename:
                f[f"sig_{role}_img"] = _save_sig(up, folder, role)
                done.append(label)
                nm = (f.get(f"sig_{role}_name") or "").strip()
                if nm:  # เก็บเข้าคลังลายเซ็นของบริษัท (ชื่อเดิม = แทนที่)
                    lib_dir = os.path.join(current_app.config["DATA_DIR"], "siglib", str(job.company_id))
                    os.makedirs(lib_dir, exist_ok=True)
                    lp = os.path.join(lib_dir, uuid.uuid4().hex[:10] + ".png")
                    shutil.copy(f[f"sig_{role}_img"], lp)
                    old_ = SigLib.query.filter_by(company_id=job.company_id, name=nm).first()
                    if old_:
                        old_.path, old_.position = lp, f.get("sig_sender_pos", "") if role == "sender" else old_.position
                    else:
                        db.session.add(SigLib(company_id=job.company_id, name=nm, path=lp, created_by=current_user.id,
                                              position=f.get("sig_sender_pos", "") if role == "sender" else ""))
                    f[f"sig_{role}_img"] = lp
        except ValueError as e:
            flash(str(e), "error")
        lib_id = request.form.get(f"sig_{role}_lib", type=int)
        if lib_id and not (up and up.filename):
            it = db.session.get(SigLib, lib_id)
            if it and it.company_id == job.company_id and os.path.exists(it.path):
                f[f"sig_{role}_img"], f[f"sig_{role}_name"] = it.path, it.name
                if role == "sender" and it.position and not f.get("sig_sender_pos"):
                    f["sig_sender_pos"] = it.position
                done.append(f"{label} (จากคลัง: {it.name})")
        if request.form.get(f"sig_{role}_mine") == "1" and current_user.signature_path:
            f[f"sig_{role}_img"] = current_user.signature_path
            f[f"sig_{role}_name"] = f.get(f"sig_{role}_name") or current_user.name
            if role == "sender" and not f.get("sig_sender_pos"):
                f["sig_sender_pos"] = current_user.position or ""
            done.append(label + " (ลายเซ็นของฉัน)")
        if request.form.get(f"sig_{role}_clear") == "1":
            f.pop(f"sig_{role}_img", None)
    job.setj("fields", f)
    job.updated_by = current_user.id
    log("signature", "บันทึกผู้ลงนาม" + (": ลายเซ็น " + ", ".join(done) if done else ""), job)
    db.session.commit()
    flash("บันทึกผู้ลงนามแล้ว", "ok")
    return redirect(url_for("main.job_view", job_id=job.id) + "#sign")


@bp.route("/jobs/<int:job_id>/sig/<role>")
@login_required
def job_sig_img(job_id, role):
    job = get_job(job_id)
    p = job.fields.get(f"sig_{role}_img")
    if not p or not os.path.exists(p):
        abort(404)
    return send_file(p, mimetype="image/png")


@bp.route("/profile", methods=["GET", "POST"])
@login_required
def profile():
    u = current_user
    if request.method == "POST":
        u.position = request.form.get("position", "").strip()
        up = request.files.get("signature")
        if up and up.filename:
            try:
                u.signature_path = _save_sig(up, os.path.join(current_app.config["DATA_DIR"], "signatures"), f"u{u.id}")
                log("profile", "อัปโหลดลายเซ็นของตัวเอง")
            except ValueError as e:
                flash(str(e), "error")
        pw = request.form.get("new_password", "")
        if pw:
            if len(pw) < 8 or not u.check_password(request.form.get("old_password", "")):
                flash("เปลี่ยนรหัสไม่สำเร็จ: รหัสเดิมไม่ถูก หรือรหัสใหม่สั้นกว่า 8 ตัว", "error")
            else:
                u.set_password(pw)
                log("profile", "เปลี่ยนรหัสผ่าน")
                flash("เปลี่ยนรหัสผ่านแล้ว", "ok")
        db.session.commit()
        return redirect(url_for("main.profile"))
    return render_template("profile.html")


@bp.route("/profile/sig")
@login_required
def profile_sig():
    if not current_user.signature_path or not os.path.exists(current_user.signature_path):
        abort(404)
    return send_file(current_user.signature_path, mimetype="image/png")

# ---------------------------------------------------------------- import photos


def _examples(company_id):
    rows = Example.query.filter_by(company_id=company_id).order_by(Example.created_at.desc()).all()
    out, seen = [], Counter()
    for r in rows:
        if seen[r.kind] < 3 and os.path.exists(r.path):
            out.append((r.kind, r.path))
            seen[r.kind] += 1
    return out


def _add_example(job, photo, kind):
    if not kind or not photo:
        return
    if Example.query.filter_by(company_id=job.company_id, photo_id=photo.id, kind=kind).first():
        return
    db.session.add(Example(company_id=job.company_id, kind=kind, path=photo.thumb, photo_id=photo.id))
    # เก็บล่าสุด 6 รูปต่อประเภท
    old = Example.query.filter_by(company_id=job.company_id, kind=kind).order_by(Example.created_at.desc()).offset(6).all()
    for o in old:
        db.session.delete(o)


def _store_sor(job, name, data):
    meta = parse_sor(data, name)
    d = os.path.join(job_dir(job), "files")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, uuid.uuid4().hex[:10] + ".sor")
    with open(path, "wb") as fh:
        fh.write(data)
    db.session.add(Attachment(job_id=job.id, kind="sor", filename=name, path=path, pages=1,
                              meta_json=json.dumps(meta, ensure_ascii=False), uploaded_by=current_user.id))
    f = job.fields
    if meta.get("length_km") and not f.get("lb_L"):
        f["lb_L"] = f"{meta['length_km']:.3f}"
        job.setj("fields", f)
    return meta


def do_import(job, data, filename):
    s1, s2 = sites_from_link(job.link_name)
    try:
        res = run_import(data, os.path.join(job_dir(job), "photos"), s1, s2)
    except Exception as e:  # pragma: no cover
        current_app.logger.exception("import failed")
        flash(f"Import ไม่สำเร็จ: {e}", "error")
        return
    photos = res["photos"]
    n_mem = apply_memory(job, photos)
    ai_note = run_ai(job, photos) + (f", จำจากที่เคยสอน {n_mem} รูป" if n_mem else "")
    # ใช้รูปเดิมที่ import ไว้แล้วร่วมด้วย (Import หลายรอบ)
    prev = [{"id": p.id, "rel": p.rel, "name": p.name, "group": p.group, "sub": (int(p.sub) if p.group in ("site", "plug") and p.sub.isdigit() else p.sub),
             "kind": p.kind, "kconf": p.kconf, "path": p.path, "feat": features(p.path), "exif_time": None, "lat": p.lat, "lon": p.lon}
            for p in Photo.query.filter_by(job_id=job.id).all()]
    for p in photos:
        db.session.add(Photo(id=p["id"], job_id=job.id, rel=p["rel"], name=p["name"], group=p["group"], sub=str(p["sub"] or ""),
                             path=p["path"], thumb=p["thumb"], lat=p.get("lat"), lon=p.get("lon"), kind=p.get("kind"),
                             kconf=p.get("kconf"), blurry=bool(p.get("blurry")), uploaded_by=current_user.id))
    cl = job.closures
    for c in res["closures"]:
        if c not in cl:
            cl.append(c)
    job.setj("closures", cl)
    assign_new, conf_new, method_new = auto_assign(prev + photos, closure_list(job.fields, cl))
    merge_memory(photos, set(all_slot_keys(job_pages(job))), assign_new, conf_new, method_new)
    assign, conf, method = job.assign, job.j("conf", {}), job.j("method", {})
    review = set(job.j("review", []))
    added = 0
    for slot, pid in assign_new.items():
        if not assign.get(slot):
            assign[slot] = pid
            conf[slot], method[slot] = conf_new[slot], method_new[slot]
            if conf_new[slot] < REVIEW_BELOW:
                review.add(slot)
            added += 1
    job.setj("assign", assign)
    job.setj("conf", conf)
    job.setj("method", method)
    job.setj("review", sorted(review))
    f = job.fields
    if res["kmz"]:
        job.setj("kmz", res["kmz"])
    # NE = จำนวนอัลบั้ม/โฟลเดอร์หัวต่อ
    if res["closure_folders"] and not f.get("lb_NE"):
        f["lb_NE"] = str(len(res["closure_folders"]))
    elif res["closures"] and not f.get("lb_NE"):
        f["lb_NE"] = str(len(res["closures"]))
    job.setj("fields", f)
    for name, sdata in res["sors"]:
        try:
            _store_sor(job, name, sdata)
        except Exception as e:  # pragma: no cover
            flash(f"อ่าน {name} ไม่ได้: {e}", "error")
    f = job.fields
    if not f.get("lb_L"):
        L = fiber_length_from_images(photos)
        if L:
            f["lb_L"] = f"{L:.3f}"
            job.setj("fields", f)
    db.session.flush()
    fill_closure_coords(job)
    job.updated_by = current_user.id
    unknown = sum(1 for p in photos if p["group"] == "unknown")
    log("import", f"Import {filename}: {len(photos)} รูป, วางเข้าช่องอัตโนมัติ {added} ช่อง, "
                  f"หัวต่อ {len(res['closures'])} จุด, .sor {len(res['sors'])} ไฟล์, ไม่ทราบกลุ่ม {unknown} รูป"
                  + (", มี KMZ" if res["kmz"] else "") + ai_note, job)
    db.session.commit()
    flash(f"Import {len(photos)} รูป — วางเข้าช่องให้ {added} ช่อง (ให้ตรวจ {len([s for s in review if assign.get(s)])} ช่อง){ai_note}", "ok")


def _remember(job, photo, slot):
    """จำว่ารูปนี้อยู่ช่องนี้ (ต่อบริษัท)"""
    try:
        h = dhash(photo.path)
    except OSError:
        return
    PhotoMemory.query.filter_by(company_id=job.company_id, slot=slot, hash=h).delete()
    db.session.add(PhotoMemory(company_id=job.company_id, hash=h, slot=slot, kind=slot_kind(slot)))


def _forget(job, photo, slot):
    try:
        h = dhash(photo.path)
    except OSError:
        return
    PhotoMemory.query.filter_by(company_id=job.company_id, slot=slot, hash=h).delete()


def apply_memory(job, photos):
    """ใส่ p['mem_slots'] / p['kind'] ให้รูปที่ตรงกับที่เคยสอนไว้ คืนจำนวนรูปที่จำได้"""
    mem = PhotoMemory.query.filter_by(company_id=job.company_id).all()
    if not mem:
        return 0
    by_hash = {}
    for m in mem:
        by_hash.setdefault(m.hash, []).append(m)
    n = 0
    for p in photos:
        try:
            h = dhash(p["path"])
        except OSError:
            continue
        hit = by_hash.get(h)
        if not hit:
            near = [k for k in by_hash if hamming(k, h) <= 3]
            hit = by_hash[near[0]] if len(near) == 1 else None
        if hit:
            p["mem_slots"] = [m.slot for m in hit]
            p["kind"] = next((m.kind for m in hit if m.kind), p.get("kind"))
            p["kconf"] = 1.0
            n += 1
    return n


def merge_memory(photos, valid_slots, assign_new, conf_new, method_new):
    """ให้ช่องที่จำได้ชนะผลของ AI/กฎ"""
    for p in photos:
        slots = [s_ for s_ in p.get("mem_slots", []) if s_ in valid_slots]
        if not slots:
            continue
        for s_ in [k for k, v in assign_new.items() if v == p["id"] and k not in slots and method_new.get(k) != "rule"]:
            assign_new.pop(s_)
        for s_ in slots:
            assign_new[s_], conf_new[s_], method_new[s_] = p["id"], 1.0, "memory"


def run_ai(job, photos):
    """ให้ AI จัดประเภทรูป (แก้ dict ใน photos) คืนข้อความสรุปสำหรับ log/flash"""
    if not vision.enabled() or not photos:
        return ""
    targets = [(p["id"], p["path"], os.path.dirname(p["rel"])) for p in photos
               if p["group"] not in ("design", "planwork", "route") and not p.get("mem_slots")]
    if not targets:
        return ""
    result, usage = vision.classify(targets, _examples(job.company_id))
    for p in photos:
        r = result.get(p["id"])
        if r:
            p["kind"], p["kconf"] = r.get("kind"), float(r.get("conf") or 0.8)
            if r.get("lat") and r.get("lon") and not p.get("lat"):
                try:
                    p["lat"], p["lon"] = float(r["lat"]), float(r["lon"])
                except (TypeError, ValueError):
                    pass
    f = job.fields
    f["_ai"] = {"at": now().isoformat(timespec="minutes"), "n": len(result), "total": len(targets), "usd": usage["usd"],
                "tokens": usage["in"] + usage["out"], "errors": usage.get("errors", []), "examples": len(_examples(job.company_id))}
    job.setj("fields", f)
    if usage.get("errors"):
        flash("AI ทำงานไม่สำเร็จ: " + " / ".join(usage["errors"])[:300] + " — ระบบใช้วิธีสำรอง (โฟลเดอร์/สี) แทน", "error")
    return f", AI จัดประเภท {len(result)}/{len(targets)} รูป ({usage['in'] + usage['out']:,} tokens ≈ ${usage['usd']})" + \
        (f", AI error: {usage['errors'][0]}" if usage.get("errors") else "")


def _photo_dicts(job):
    out = []
    for p in Photo.query.filter_by(job_id=job.id).all():
        sub = int(p.sub) if p.group in ("site", "plug") and (p.sub or "").isdigit() else (p.sub or None)
        out.append({"id": p.id, "rel": p.rel, "name": p.name, "group": p.group, "sub": sub, "kind": p.kind, "kconf": p.kconf,
                    "path": p.path, "feat": features(p.path), "exif_time": None, "lat": p.lat, "lon": p.lon})
    out.sort(key=photo_time_key)
    return out


@bp.route("/jobs/<int:job_id>/ai-redo", methods=["POST"])
@login_required
def job_ai_redo(job_id):
    """ให้ AI ดูรูปทั้งหมดของงานนี้ใหม่ (ใช้ตัวอย่างที่สอนล่าสุด) แล้วจัดช่องใหม่ — ไม่แตะช่องที่คนเลือกเอง"""
    job = get_job(job_id, edit=True)
    photos = _photo_dicts(job)
    method = job.j("method", {})
    manual_ids = {pid for s_, pid in job.assign.items() if method.get(s_) == "manual"}
    for p in photos:
        if p["id"] not in manual_ids:
            p["kind"] = None
    free = [p for p in photos if p["id"] not in manual_ids]
    n_mem = apply_memory(job, free)
    note = run_ai(job, free) + (f", จำจากที่เคยสอน {n_mem} รูป" if n_mem else "")
    for p in photos:
        ph = db.session.get(Photo, p["id"])
        if ph and p["id"] not in manual_ids:
            ph.kind, ph.kconf = p.get("kind"), p.get("kconf")
    new_a, new_c, new_m = auto_assign(photos, closure_list(job.fields, job.closures))
    merge_memory(free, set(all_slot_keys(job_pages(job))), new_a, new_c, new_m)
    assign, conf = job.assign, job.j("conf", {})
    review = {k for k in job.j("review", []) if method.get(k) == "manual"}
    changed = 0
    for slot in set(assign) | set(new_a):
        if method.get(slot) == "manual":
            continue
        if assign.get(slot) != new_a.get(slot):
            changed += 1
        if slot in new_a:
            assign[slot], conf[slot], method[slot] = new_a[slot], new_c[slot], new_m[slot]
            if new_c[slot] < REVIEW_BELOW:
                review.add(slot)
        else:
            assign.pop(slot, None)
    job.setj("assign", assign)
    job.setj("conf", conf)
    job.setj("method", method)
    job.setj("review", sorted(review))
    job.updated_by = current_user.id
    log("ai_redo", f"ให้ AI จัดรูปใหม่: เปลี่ยน {changed} ช่อง (ไม่แตะช่องที่เลือกเอง {len(manual_ids)} รูป){note}", job)
    db.session.commit()
    flash(f"AI จัดรูปใหม่แล้ว — เปลี่ยน {changed} ช่อง{note}", "ok")
    return redirect(url_for("main.job_photos", job_id=job.id))


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
        import zipfile
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            for f in files:
                z.writestr(f.filename, f.read())
        do_import(job, buf.getvalue(), f"{len(files)} ไฟล์")
    return redirect(url_for("main.job_photos", job_id=job.id))

# ---------------------------------------------------------------- closure coordinates / KMZ


def closure_coords(job):
    a = job.assign
    design = {}
    for n, la, lo in job.j("kmz", []):
        c = norm_closure(n)
        if c:
            design[c] = (la, lo)
    manual = job.fields.get("_coords", {})
    out = []
    for c in closure_list(job.fields, job.closures):
        pid = a.get(f"closure:{c}:pole")
        p = db.session.get(Photo, pid) if pid and pid != "NA" else None
        lat, lon, src = None, None, ""
        if c in manual:
            lat, lon, src = manual[c][0], manual[c][1], "กรอกเอง"
        elif p and p.lat:
            lat, lon, src = p.lat, p.lon, "จากรูป"
        d = None
        if lat is not None and c in design:
            d = round(haversine((lat, lon), design[c]))
        out.append({"closure": c, "photo": p, "lat": lat, "lon": lon, "src": src, "design": design.get(c), "dist": d})
    return out


def fill_closure_coords(job, force=False):
    """อ่านพิกัดบนรูปจุดติดตั้ง closure (รูปที่ 3) ที่ยังไม่มีพิกัด"""
    design = {norm_closure(n): (la, lo) for n, la, lo in job.j("kmz", []) if norm_closure(n)}
    for slot, pid in job.assign.items():
        if not slot.endswith(":pole") or not pid or pid == "NA":
            continue
        p = db.session.get(Photo, pid)
        if not p or (p.lat and not force):
            continue
        ll = ocr_latlon(p.path, near=design.get(slot.split(":")[1]))
        if not ll and vision.enabled():
            t = vision.read_text(p.path, "อ่านพิกัด latitude, longitude ที่พิมพ์บนรูปนี้ ตอบในรูปแบบ lat,lon เท่านั้น ถ้าไม่มีตอบ none")
            m = re.search(r"(1\d\.\d{3,})\D+(1\d\d\.\d{3,})", t or "")
            ll = (float(m.group(1)), float(m.group(2))) if m else None
        if ll:
            p.lat, p.lon = ll


@bp.route("/jobs/<int:job_id>/coords", methods=["POST"])
@login_required
def job_coords(job_id):
    job = get_job(job_id, edit=True)
    f = job.fields
    manual = f.get("_coords", {})
    for c in closure_list(f, job.closures):
        v = request.form.get(f"ll_{c}", "").strip()
        if v:
            m = re.match(r"\s*\+?(-?\d+\.\d+)\s*[, ]\s*\+?(-?\d+\.\d+)", v)
            if m:
                manual[c] = [float(m.group(1)), float(m.group(2))]
        elif c in manual and request.form.get(f"clr_{c}"):
            manual.pop(c)
    f["_coords"] = manual
    job.setj("fields", f)
    if request.form.get("reocr"):
        fill_closure_coords(job, force=True)
    log("coords", "แก้พิกัดจุดติดตั้ง closure", job)
    db.session.commit()
    flash("บันทึกพิกัดแล้ว", "ok")
    return redirect(url_for("main.job_view", job_id=job.id) + "#kmz")


@bp.route("/jobs/<int:job_id>/closures.kmz")
@login_required
def job_kmz(job_id):
    import zipfile
    job = get_job(job_id)
    rows = closure_coords(job)
    pm = []
    z = io.BytesIO()
    with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as zf:
        for r in rows:
            if r["lat"] is None:
                continue
            img = ""
            if r["photo"]:
                fn = f"files/{r['closure'].replace('#', '')}.jpg"
                zf.write(r["photo"].thumb, fn)
                img = f'<img src="{fn}" width="320"/><br/>'
            dist = f"ห่างจากจุดในแบบ {r['dist']} ม.<br/>" if r["dist"] is not None else ""
            desc = f"<![CDATA[{img}<b>{r['closure']}</b><br/>{r['lat']:.6f}, {r['lon']:.6f}<br/>{dist}ที่มา: {r['src']}]]>"
            pm.append(f"<Placemark><name>{r['closure']}</name><styleUrl>#cl</styleUrl><description>{desc}</description>"
                      f"<Point><coordinates>{r['lon']:.6f},{r['lat']:.6f},0</coordinates></Point></Placemark>")
        kml = (f'<?xml version="1.0" encoding="UTF-8"?><kml xmlns="http://www.opengis.net/kml/2.2"><Document>'
               f"<name>{html_escape(job.link_name)} - จุดติดตั้ง closure</name>"
               f'<Style id="cl"><IconStyle><scale>1.1</scale><Icon><href>http://maps.google.com/mapfiles/kml/pushpin/ylw-pushpin.png</href></Icon><hotSpot x="20" y="2" xunits="pixels" yunits="pixels"/></IconStyle></Style>'
               f"{''.join(pm)}</Document></kml>")
        zf.writestr("doc.kml", kml)
    log("kmz", f"ดาวน์โหลด KMZ จุดติดตั้ง closure ({len(pm)} จุด)", job)
    db.session.commit()
    z.seek(0)
    return send_file(z, as_attachment=True, download_name=safe_name(job.link_name.replace(" ", "_")) + "_closures.kmz",
                     mimetype="application/vnd.google-earth.kmz")

# ---------------------------------------------------------------- photo review


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
                 "design": "Design", "route": "Route", "otdr": "OTDR", "unknown": "ไม่ทราบกลุ่ม"}.get(p.group, p.group)
        key = os.path.dirname(p.rel) or "(root)"
        groups.setdefault(key, {"label": label, "items": []})["items"].append(p)
    cfg = job.j("sheet_cfg", {})
    sheet_n = len(pdfgen.parse_ranges(cfg.get("forms", ""), cfg.get("count", 0))) or 3
    pno, n = [], 1
    for p in pages:
        pno.append(n)
        n += sheet_n if p["kind"] == "sheet" else 1
    s1, s2 = sites_from_link(job.link_name)
    c1, c2 = s1.split("_", 1)[-1], s2.split("_", 1)[-1]

    def pref(key):
        if key.startswith("site1"):
            return c1
        if key.startswith("site2"):
            return c2
        if key.startswith("plug1"):
            return "PLUG*" + c1
        if key.startswith("plug2"):
            return "PLUG*" + c2
        if key.startswith("closure:"):
            return key.split(":")[1]
        if key.startswith("power"):
            return "POWER"
        if key == "planwork":
            return "PLAN"
        return ""
    prefs = {k: pref(k) for k in all_slot_keys(pages)}
    return render_template("photos.html", job=job, pages=pages, pno=pno, prefs=prefs, photos=photos, used=used, groups=groups,
                           assign=job.assign, conf=job.j("conf", {}), method=job.j("method", {}),
                           review=set(job.j("review", [])), prog=progress(job, pages), STATUS=STATUS, ai=vision.enabled(),
                           ai_info=job.fields.get("_ai"), n_examples=len(_examples(job.company_id)), KIND_TH=KIND_TH,
                           n_memory=PhotoMemory.query.filter_by(company_id=job.company_id).count())


def _set_slot(job, slot, pid, how="manual"):
    a, method, conf, review = job.assign, job.j("method", {}), job.j("conf", {}), set(job.j("review", []))
    if pid in ("", None):
        a.pop(slot, None)
    else:
        a[slot] = pid
    method[slot], conf[slot] = how, 1.0
    review.discard(slot)
    # Site รอบที่ 2 ใช้รูปชุดเดียวกับ Plug Port (ถ้ายังเป็นค่าที่ระบบตั้งตามกฎ)
    m = re.match(r"plug(\d):(location|odf1|odf2)$", slot)
    if m:
        dst = f"site{m.group(1)}:r2:" + {"location": "location", "odf1": "odf", "odf2": "core"}[m.group(2)]
        if method.get(dst) in ("rule", None):
            if pid:
                a[dst] = pid
            else:
                a.pop(dst, None)
            method[dst], conf[dst] = "rule", 1.0
            review.discard(dst)
    job.setj("assign", a)
    job.setj("method", method)
    job.setj("conf", conf)
    job.setj("review", sorted(review))
    if pid and pid != "NA":
        p = db.session.get(Photo, pid)
        k = slot_kind(slot)
        if p:
            _remember(job, p, slot)
        if p and k:
            p.kind = k
            _add_example(job, p, k)


@bp.route("/jobs/<int:job_id>/assign", methods=["POST"])
@login_required
def job_assign(job_id):
    job = get_job(job_id, edit=True)
    slot = request.form.get("slot", "")
    pid = request.form.get("photo", "")
    if slot not in all_slot_keys(job_pages(job)):
        return jsonify(ok=False, error="slot"), 400
    if pid not in ("", "NA"):
        p = db.session.get(Photo, pid)
        if not p or p.job_id != job.id:
            return jsonify(ok=False, error="photo"), 400
    old = job.assign.get(slot)
    _set_slot(job, slot, pid)
    job.updated_by = current_user.id
    log("assign", f"{slot}: {old or '-'} → {pid or 'ว่าง'}", job)
    db.session.commit()
    return jsonify(ok=True, prog=progress(job), assign=job.assign)


@bp.route("/jobs/<int:job_id>/swap", methods=["POST"])
@login_required
def job_swap(job_id):
    """ลากรูปจากช่องหนึ่งไปอีกช่อง: ถ้าช่องปลายทางมีรูป จะสลับกัน"""
    job = get_job(job_id, edit=True)
    a_, b_ = request.form.get("from", ""), request.form.get("to", "")
    keys = all_slot_keys(job_pages(job))
    if a_ not in keys or b_ not in keys or a_ == b_:
        return jsonify(ok=False), 400
    pa, pb = job.assign.get(a_), job.assign.get(b_)
    _set_slot(job, b_, pa or "")
    _set_slot(job, a_, pb or "")
    job.updated_by = current_user.id
    log("swap", f"ย้ายรูป {a_} ⇄ {b_}", job)
    db.session.commit()
    return jsonify(ok=True, prog=progress(job), assign=job.assign)


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


@bp.route("/jobs/<int:job_id>/teach", methods=["POST"])
@login_required
def job_teach(job_id):
    """ใช้การจัดรูปของงานนี้เป็นตัวอย่างสอน AI สำหรับงานถัดไปของบริษัท"""
    job = get_job(job_id)
    if not current_user.can_edit(job):
        abort(403)
    n = 0
    for slot, pid in job.assign.items():
        k = slot_kind(slot)
        if pid and pid != "NA":
            p = db.session.get(Photo, pid)
            if p:
                _remember(job, p, slot)
                n += 1
                if k:
                    p.kind = k
                    _add_example(job, p, k)
    # รูปในโฟลเดอร์ Site / Plug Port / Power ที่ "ไม่ได้ใช้" ในเอกสาร -> สอน AI ว่าเป็นรูปที่ต้องข้าม
    used = {v for v in job.assign.values() if v and v != "NA"}
    skipped = 0
    for p in Photo.query.filter(Photo.job_id == job.id, Photo.group.in_(["site", "plug", "power"])).all():
        if p.id not in used and skipped < 6:
            p.kind = p.kind if p.kind in ("sign", "sfp", "otdr_screen") else "other"
            _add_example(job, p, p.kind)
            skipped += 1
    log("teach", f"ใช้งานนี้เป็นตัวอย่างสอน AI ({n} รูป)", job)
    db.session.commit()
    flash(f"จำการจัดรูปของงานนี้แล้ว {n} ช่อง (รูปเดิม Import ซ้ำจะเข้าช่องเดิมทันที) และบันทึกเป็นตัวอย่างสอน AI — งานถัดไปของบริษัทจะใช้เทียบ (งานที่ Import ไว้แล้ว กด ‘ให้ AI จัดรูปใหม่’ เพื่อจัดใหม่)", "ok")
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


ATT_KIND = {"sheet": "PDF จาก Google Sheet", "asbuilt": "แบบ As-Built (ปก/Symbol/Core Assignment)", "sor": "ไฟล์ OTDR .sor"}


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
        if kind == "sor":
            try:
                meta = _store_sor(job, f.filename, data)
                log("attach", f"แนบ .sor: {f.filename} (ยาว {meta.get('length_km')} km, Total loss {meta.get('total_loss')} dB)", job)
            except Exception as e:
                flash(f"{f.filename}: อ่านไฟล์ .sor ไม่ได้ ({e})", "error")
            continue
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
            flash(f"ตรวจพบหน้า Loss / ใบสรุปเคเบิลรื้อถอน / Balance Cable ที่หน้า {cfg['forms'] or '-'} (จาก {cfg['count']} หน้า)", "ok")
        db.session.add(Attachment(job_id=job.id, kind=kind, filename=f.filename, path=path, pages=pdfgen.count_pages(data),
                                  uploaded_by=current_user.id))
        log("attach", f"แนบ {ATT_KIND[kind]}: {f.filename}", job)
    job.updated_by = current_user.id
    db.session.commit()
    return redirect(url_for("main.job_view", job_id=job.id) + "#files")


@bp.route("/jobs/<int:job_id>/sheet-cfg", methods=["POST"])
@login_required
def job_sheet_cfg(job_id):
    job = get_job(job_id, edit=True)
    cfg = job.j("sheet_cfg", {})
    cfg["forms"] = request.form.get("forms", "").strip()
    job.setj("sheet_cfg", cfg)
    log("sheet_cfg", f"ตั้งหน้าที่ใช้จาก Google Sheet: {cfg['forms']}", job)
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


def html_escape(s):
    import html
    return html.escape(s or "")


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
    if sheet_bytes and job.j("sheet_cfg", {}).get("v") != 2:  # งานที่แนบไว้ก่อนอัปเดต: หาหน้าใหม่ตามรูปแบบใหม่
        job.setj("sheet_cfg", pdfgen.detect_sheet_pages(sheet_bytes))
    sors = [a.meta for a in sorted([a for a in atts if a.kind == "sor"], key=lambda a: a.filename) if a.meta]
    asb = [open(a.path, "rb").read() for a in atts if a.kind == "asbuilt"]
    jobd = {"link_name": job.link_name, "fields": {**DEFAULT_FIELDS, **job.fields}, "template_version": "3.0"}
    try:
        pat, asbuilt = pdfgen.build_docs(jobd, comp, pages, by_slot, company.template_logo_path or None, company.logo_path or None,
                                         sheet_bytes, job.j("sheet_cfg", {}), sors, asb)
    except Exception as e:
        current_app.logger.exception("generate failed")
        flash(f"สร้าง PDF ไม่สำเร็จ: {e}", "error")
        return redirect(url_for("main.job_view", job_id=job.id))
    ver = (db.session.query(func.max(Document.version)).filter_by(job_id=job.id).scalar() or 0) + 1
    d = os.path.join(job_dir(job), "out")
    os.makedirs(d, exist_ok=True)
    base = safe_name(job.link_name.replace(" ", "_"))
    for kind, data, prefix in (("PAT", pat, "PAT_"), ("ASBUILT", asbuilt, "PATAS-BUILT_")):
        fn = f"{prefix}{base}.pdf"
        path = os.path.join(d, f"v{ver}_{fn}")
        with open(path, "wb") as fh:
            fh.write(data)
        db.session.add(Document(job_id=job.id, kind=kind, version=ver, filename=fn, path=path,
                                pages=pdfgen.count_pages(data), size=len(data), created_by=current_user.id))
    if job.status in ("draft", "rejected"):
        job.status = "ready"
    job.updated_by = current_user.id
    log("generate", f"สร้าง PDF v{ver}: PAT {pdfgen.count_pages(pat)} หน้า, AS-BUILT {pdfgen.count_pages(asbuilt)} หน้า", job)
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
        opts = defaults.get("options") or {}
        for k in DROPDOWN:
            raw = request.form.get("opt_" + k)
            if raw is not None:
                seen = []
                for line in raw.replace("\r", "").split("\n"):
                    line = line.strip()
                    if line and line not in seen:
                        seen.append(line)
                opts[k] = seen
        defaults["options"] = opts
        c.defaults_json = json.dumps(defaults, ensure_ascii=False)
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
    return render_template("companies.html", companies=cs, DROPDOWN=DROPDOWN, SEED=SEED_OPTIONS)


@bp.route("/logo/<int:cid>/<which>")
@login_required
def logo(cid, which):
    c = db.session.get(Company, cid) or abort(404)
    if not current_user.is_super and c.id != current_user.company_id:
        abort(404)
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
    show_all = request.args.get("all") == "1"
    raw = q.order_by(Audit.created_at.desc()).limit(800).all()
    rows = present.timeline(raw, show_all)
    jobs = {j.id: j for j in Job.query.filter(Job.id.in_({r["job_id"] for r in rows if r["job_id"]})).all()}
    return render_template("audit.html", rows=rows, jobs=jobs, show_all=show_all, who=who, hidden=len(raw) - sum(r["n"] for r in rows))


# ---------------------------------------------------------------- ลบข้อมูล / ล้างประวัติ


def _rm(path):
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


@bp.route("/siglib/<int:sid>/img")
@login_required
def siglib_img(sid):
    it = db.session.get(SigLib, sid) or abort(404)
    if not current_user.is_super and it.company_id != current_user.company_id:
        abort(404)
    return send_file(it.path, mimetype="image/png")


@bp.route("/siglib/<int:sid>/delete", methods=["POST"])
@login_required
def siglib_delete(sid):
    it = db.session.get(SigLib, sid) or abort(404)
    if not current_user.can_edit() or (not current_user.is_super and it.company_id != current_user.company_id):
        abort(403)
    log("siglib_delete", f"ลบลายเซ็นในคลัง: {it.name}", company_id=it.company_id)
    db.session.delete(it)   # ไม่ลบไฟล์ เพราะงานเดิมอาจยังใช้อยู่
    db.session.commit()
    flash("ลบลายเซ็นออกจากคลังแล้ว", "ok")
    return redirect(request.form.get("next") or url_for("main.dashboard"))


@bp.route("/docs/<int:doc_id>/delete", methods=["POST"])
@login_required
def doc_delete(doc_id):
    d = db.session.get(Document, doc_id) or abort(404)
    job = get_job(d.job_id, edit=True)
    log("doc_delete", f"ลบ PDF {d.kind} v{d.version}", job)
    _rm(d.path)
    db.session.delete(d)
    db.session.commit()
    return redirect(url_for("main.job_view", job_id=job.id) + "#docs")


@bp.route("/jobs/<int:job_id>/docs-clear", methods=["POST"])
@login_required
def docs_clear(job_id):
    job = get_job(job_id, edit=True)
    docs = Document.query.filter_by(job_id=job.id).all()
    keep = max((d.version for d in docs), default=0) if request.form.get("keep_latest") else None
    n = 0
    for d in docs:
        if d.version != keep:
            _rm(d.path)
            db.session.delete(d)
            n += 1
    log("doc_delete", f"ลบ PDF เก่า {n} ไฟล์" + (" (เก็บเวอร์ชันล่าสุด)" if keep else ""), job)
    db.session.commit()
    flash(f"ลบ PDF แล้ว {n} ไฟล์", "ok")
    return redirect(url_for("main.job_view", job_id=job.id) + "#docs")


def _delete_photo(job, p):
    a = job.assign
    for slot in [k for k, v in a.items() if v == p.id]:
        a.pop(slot)
    job.setj("assign", a)
    Example.query.filter_by(photo_id=p.id).delete()
    _rm(p.path)
    _rm(p.thumb)
    db.session.delete(p)


@bp.route("/photo/<pid>/delete", methods=["POST"])
@login_required
def photo_delete(pid):
    p = db.session.get(Photo, pid) or abort(404)
    job = get_job(p.job_id, edit=True)
    name = p.rel
    _delete_photo(job, p)
    log("photo_delete", f"ลบรูป {name}", job)
    db.session.commit()
    return jsonify(ok=True)


@bp.route("/jobs/<int:job_id>/photos-clear", methods=["POST"])
@login_required
def photos_clear(job_id):
    job = get_job(job_id, edit=True)
    ps = Photo.query.filter_by(job_id=job.id).all()
    for p in ps:
        _delete_photo(job, p)
    for k in ("assign", "conf", "method"):
        job.setj(k, {})
    job.setj("review", [])
    log("photo_delete", f"ลบรูปทั้งหมดของงาน ({len(ps)} รูป)", job)
    db.session.commit()
    flash(f"ลบรูปทั้งหมดแล้ว {len(ps)} รูป — Import ใหม่ได้เลย", "ok")
    return redirect(url_for("main.job_view", job_id=job.id) + "#import")


@bp.route("/examples-clear", methods=["POST"])
@login_required
def examples_clear():
    admin_required()
    q = Example.query
    if not current_user.is_super:
        q = q.filter(Example.company_id == current_user.company_id)
    n = q.delete()
    mq = PhotoMemory.query
    if not current_user.is_super:
        mq = mq.filter(PhotoMemory.company_id == current_user.company_id)
    mq.delete()
    log("examples_clear", f"ล้างรูปตัวอย่างสอน AI {n} รูป")
    db.session.commit()
    flash(f"ล้างรูปตัวอย่างสอน AI แล้ว {n} รูป", "ok")
    return redirect(request.form.get("next") or url_for("main.dashboard"))


@bp.route("/jobs/<int:job_id>/delete", methods=["POST"])
@login_required
def job_delete(job_id):
    job = get_job(job_id)
    if not current_user.can_approve(job):
        abort(403)
    log("delete", f"ลบงาน {job.link_name}", job)
    for p in Photo.query.filter_by(job_id=job.id).all():
        Example.query.filter_by(photo_id=p.id).delete()
        db.session.delete(p)
    Attachment.query.filter_by(job_id=job.id).delete()
    Document.query.filter_by(job_id=job.id).delete()
    shutil.rmtree(job_dir(job), ignore_errors=True)
    db.session.delete(job)
    db.session.commit()
    flash("ลบงานแล้ว", "ok")
    return redirect(url_for("main.dashboard"))


@bp.route("/audit/clear", methods=["POST"])
@login_required
def audit_clear():
    admin_required()
    q = Audit.query
    if not current_user.is_super:
        q = q.filter(Audit.company_id == current_user.company_id)
    scope = request.form.get("scope", "all")
    if scope == "old":
        from datetime import timedelta
        q = q.filter(Audit.created_at < now() - timedelta(days=30))
    elif scope == "noise":
        q = q.filter(Audit.action.in_(["login", "logout", "download", "assign", "swap", "kmz"]))
    n = q.delete(synchronize_session=False)
    label = {"all": "ทั้งหมด", "old": "ที่เก่ากว่า 30 วัน", "noise": "เฉพาะ login/ดาวน์โหลด/ย้ายรูป"}.get(scope, scope)
    log("audit_clear", f"ล้าง Audit log {label}: {n} รายการ")
    db.session.commit()
    flash(f"ล้าง Audit log แล้ว {n} รายการ", "ok")
    return redirect(url_for("main.audit"))
