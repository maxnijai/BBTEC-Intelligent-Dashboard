"""
Import รูปจาก ZIP / โฟลเดอร์ แล้วจับคู่เข้าช่องในเอกสารอัตโนมัติ

ลำดับการหาช่องของรูป:
 1) ชื่อโฟลเดอร์ (PHE1149/, Closure/SJ#01/, PlugPort/PHE0020/, Power/, PlanWork/, Design.jpg ...)
 2) OCR พิกัดบนรูป (ตัวอักษรสีส้ม/ขาวจากแอปกล้อง) -> เทียบจุดที่ใกล้สุดใน KMZ
 3) ภายในหัวต่อ: แยก หน้า closure / ด้านใน / จุดติดตั้ง ด้วยการวิเคราะห์สีภาพ (ท้องฟ้า / ถาดสีขาว / กล่องเทา)
ทุกช่องมีค่า confidence; ต่ำกว่าเกณฑ์จะขึ้นในคิว "ให้คนตรวจ"
"""
import io
import itertools
import math
import os
import re
import uuid
import zipfile

import numpy as np
from PIL import Image, ImageFilter, ImageOps

try:
    import pillow_heif
    pillow_heif.register_heif_opener()
except Exception:  # pragma: no cover
    pass

try:
    import pytesseract
except Exception:  # pragma: no cover
    pytesseract = None

from .layout import site_code

IMG_EXT = (".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp")
CLOSURE_RX = re.compile(r"\b([SB]J)\s*#?\s*0*(\d{1,2})\b", re.I)
REVIEW_BELOW = 0.75

# ---------------------------------------------------------------- KMZ


def parse_kmz(data):
    """คืนค่า list ของ (name, lat, lon) ตามลำดับในไฟล์"""
    try:
        z = zipfile.ZipFile(io.BytesIO(data))
        kml = next((n for n in z.namelist() if n.lower().endswith(".kml")), None)
        text = z.read(kml).decode("utf-8", "ignore") if kml else ""
    except zipfile.BadZipFile:
        text = data.decode("utf-8", "ignore")
    pts = []
    for m in re.finditer(r"<Placemark>(.*?)</Placemark>", text, re.S):
        body = m.group(1)
        name = re.search(r"<name>(.*?)</name>", body, re.S)
        pt = re.search(r"<Point>.*?<coordinates>\s*([-\d.]+),([-\d.]+)", body, re.S)
        if name and pt:
            pts.append((name.group(1).strip(), float(pt.group(2)), float(pt.group(1))))
    return pts


def norm_closure(name):
    m = CLOSURE_RX.search(name or "")
    if not m:
        return None
    return f"{m.group(1).upper()}#{int(m.group(2)):02d}"


def haversine(a, b):
    R = 6371000
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    d = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 2 * R * math.asin(math.sqrt(d))

# ---------------------------------------------------------------- image utils


def open_image(path, max_side=None):
    im = Image.open(path)
    if max_side and im.format == "JPEG":
        # ให้ตัวถอด JPEG ย่อขนาดตั้งแต่ตอนอ่าน (เร็วขึ้นหลายเท่า)
        im.draft("RGB", (max_side, max_side))
    im = ImageOps.exif_transpose(im)
    return im.convert("RGB")


def save_variants(src_path, out_dir, stem):
    """สร้างรูปสำหรับ PDF (ยาวสุด 1400px) และ thumbnail 360px"""
    im = open_image(src_path, 1150)
    big = im.copy()
    big.thumbnail((1150, 1150))
    big_path = os.path.join(out_dir, stem + ".jpg")
    big.save(big_path, "JPEG", quality=72, optimize=True)
    th = im.copy()
    th.thumbnail((360, 360))
    th_path = os.path.join(out_dir, stem + "_t.jpg")
    th.save(th_path, "JPEG", quality=75)
    exif_time = None
    try:
        exif = Image.open(src_path).getexif()
        exif_time = exif.get(306)  # DateTime
    except Exception:
        pass
    return big_path, th_path, big.size, exif_time


def features(path):
    im = Image.open(path).convert("RGB")
    im.thumbnail((300, 300))
    a = np.asarray(im).astype(int)
    h = a.shape[0]
    top = a[: h // 3]
    tr, tg, tb = top[..., 0], top[..., 1], top[..., 2]
    sky = float((((tb > tr + 10) & (tb > 130)) | ((tr > 200) & (tg > 200) & (tb > 200))).mean())
    c = a[h // 4: 3 * h // 4]
    cmx, cmn = c.max(-1), c.min(-1)
    white = float(((cmn > 170) & (cmx - cmn < 35)).mean())
    mx, mn = a.max(-1), a.min(-1)
    gray = float((((mx - mn) < 30) & (mx > 60) & (mx < 170)).mean())
    # ความคมชัด (variance of Laplacian แบบง่าย) ใช้เตือนรูปเบลอ
    g = np.asarray(im.convert("L").filter(ImageFilter.FIND_EDGES)).astype(float)
    sharp = float(g.var())
    return {"sky": sky, "white": white, "gray": gray, "sharp": sharp}


GPS_RX = re.compile(r"\+?\s*(1[0-9])[.,](\d{4,})\D{0,4}?\+?\s*(\d{3})[.,](\d{4,})")


def ocr_latlon(path, near=None):
    """อ่านพิกัดที่แอปกล้องพิมพ์ไว้บนรูป (มุมขวาบน หรือ ขวาล่าง)
    อ่านหลายแบบ (หลาย threshold/psm) แล้วโหวต; ถ้ารู้จุดที่ควรอยู่ (near) เลือกค่าที่ใกล้สุด"""
    if pytesseract is None:
        return None
    im = Image.open(path).convert("RGB")
    w, h = im.size
    cands = []
    for box in ((int(w * .35), 0, w, int(h * .2)), (int(w * .25), int(h * .78), w, h)):
        c = np.asarray(im.crop(box)).astype(int)
        r, g, b = c[..., 0], c[..., 1], c[..., 2]
        for mk in ((r > 170) & (r - b > 70) & (g > 80), (r > 200) & (g > 200) & (b > 200), (r > 150) & (r - b > 50),
                   (r > 190) & (r - b > 90)):
            bw = Image.fromarray(np.where(mk, 0, 255).astype("uint8"))
            for scale, psm in ((2, 6), (3, 7)):
                x = bw.resize((bw.width * scale, bw.height * scale)).filter(ImageFilter.MedianFilter(3))
                try:
                    t = pytesseract.image_to_string(x, lang="eng", config=f"--psm {psm} -c tessedit_char_whitelist=0123456789.,+:-/ ")
                except Exception:
                    return None
                for m in GPS_RX.finditer(t):
                    lat = float(m.group(1) + "." + m.group(2)[:6])
                    lon = float(m.group(3) + "." + m.group(4)[:6])
                    if 5 < lat < 21 and 97 < lon < 106:
                        cands.append((round(lat, 6), round(lon, 6)))
                # พอแล้วเมื่ออ่านได้ค่าเดิมซ้ำ 2 ครั้ง (และใกล้จุดในแบบ ถ้ามี)
                if cands:
                    from collections import Counter as _C
                    top, n = _C(cands).most_common(1)[0]
                    if n >= 2 and (not near or haversine(top, near) < 300):
                        return top
    if not cands:
        return None
    if near:
        return min(cands, key=lambda ll: haversine(ll, near))
    from collections import Counter as _C
    return _C(cands).most_common(1)[0][0]


# ---------------------------------------------------------------- classify folder of closure photos


def assign_closure_roles(photos):
    """photos: list of dict(feat) -> {photo_index: (role, confidence)}  เลือกการจัดที่คะแนนรวมสูงสุด"""
    roles = ["front", "inside", "pole"]
    score = {"front": lambda f: f["gray"], "inside": lambda f: f["white"], "pole": lambda f: f["sky"]}
    n = len(photos)
    if n == 0:
        return {}
    best, second, best_perm = -1, -1, None
    idxs = list(range(n))
    for perm in itertools.permutations(idxs, min(3, n)):
        s = sum(score[roles[i]](photos[p]["feat"]) for i, p in enumerate(perm))
        if s > best:
            best, second, best_perm = s, best, perm
        elif s > second:
            second = s
    margin = best - max(second, 0)
    conf = 0.5 + min(0.45, margin * 1.5)
    return {p: (roles[i], round(conf, 2)) for i, p in enumerate(best_perm)}

# ---------------------------------------------------------------- เวลาถ่าย (ใช้เรียงลำดับ)
from datetime import datetime


def photo_time_key(p):
    """EXIF > TimePhoto_YYYYMMDD_HHMMSS > ชื่อไฟล์ LINE (epoch ms 13 หลัก) > ชื่อไฟล์"""
    t = None
    if p.get("exif_time"):
        try:
            t = datetime.strptime(str(p["exif_time"]).strip(), "%Y:%m:%d %H:%M:%S").timestamp()
        except ValueError:
            t = None
    if t is None:
        m = re.search(r"(20\d{6})[_-]?(\d{6})", p["name"])
        if m:
            try:
                t = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").timestamp()
            except ValueError:
                t = None
    if t is None:
        m = re.search(r"(1[6-9]\d{11})", p["name"])
        if m:
            t = int(m.group(1)) / 1000.0
    return (0 if t is not None else 1, t or 0, p["name"])


def classify_path(rel, s1, s2):
    """จากพาธในไฟล์ ZIP บอกว่ารูปอยู่กลุ่มไหน"""
    parts = [p for p in re.split(r"[\\/]", rel) if p]
    up = [p.upper() for p in parts]
    name = up[-1]
    joined = "/".join(up)
    c1, c2 = site_code(s1), site_code(s2)
    cl = None
    for p in reversed(parts[:-1]):
        cl = norm_closure(p)
        if cl:
            break
    if cl is None:
        cl = norm_closure(parts[-1].rsplit(".", 1)[0])
    if cl:
        return ("closure", cl)
    if "PLANWORK" in joined or "PLAN_WORK" in joined or "PLAN WORK" in joined:
        return ("planwork", None)
    if name.startswith("DESIGN") or "TOPO" in name or "TOPE" in name:
        return ("design", None)
    if name.startswith("ROUTE"):
        return ("route", None)
    site = 1 if c1 in joined else 2 if c2 in joined else None
    if "PLUG" in joined:
        return ("plug", site)
    if "OTDR" in joined:
        return ("otdr", None)
    if "POWER" in joined or "OPM" in joined:
        return ("power", None)
    if site:
        return ("site", site)
    return ("unknown", None)


# ---------------------------------------------------------------- main import

KIND_TH = {"site_location": "Location", "odf_rack": "ตู้ ODF", "core_label": "ป้าย Core", "equipment_port": "พอร์ตอุปกรณ์",
           "power_meter": "Power meter", "otdr_screen": "จอ OTDR", "closure_front": "หน้า closure", "closure_inside": "ใน closure",
           "closure_install": "จุดติดตั้ง", "sign": "ป้าย (ไม่ใช้)", "sfp": "SFP (ไม่ใช้)", "other": "อื่น ๆ"}

SLOT_KIND = {  # ประเภทรูปที่ควรอยู่ในช่อง (เรียนรู้จาก PAT ตัวอย่าง)
    "location": "site_location", "odf": "odf_rack", "core": "core_label",
    "port1": "equipment_port", "port2": "equipment_port", "odf1": "odf_rack", "odf2": "core_label",
    "front": "closure_front", "inside": "closure_inside", "pole": "closure_install",
}


def slot_kind(slot):
    if slot.startswith("power:"):
        return "power_meter"
    return SLOT_KIND.get(slot.split(":")[-1])


def extract_zip(zip_bytes, job_dir, s1, s2):
    """แตก ZIP -> (photos, kmz_points, sor_files[(name, bytes)], closure_folders)"""
    os.makedirs(job_dir, exist_ok=True)
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))
    photos, kmz_pts, sors, folders = [], [], [], []
    tmp_dir = os.path.join(job_dir, "_src")
    os.makedirs(tmp_dir, exist_ok=True)
    for info in z.infolist():
        rel = info.filename
        if "__MACOSX" in rel:
            continue
        for part in [p for p in re.split(r"[\\/]", rel)[:-1] if p] + ([rel.rstrip("/").split("/")[-1]] if info.is_dir() else []):
            c = norm_closure(part)
            if c and c not in folders:
                folders.append(c)
        if info.is_dir():
            continue
        base = os.path.basename(rel)
        if base.startswith("."):
            continue
        low = base.lower()
        if low.endswith((".kmz", ".kml")):
            kmz_pts = parse_kmz(z.read(info))
            continue
        if low.endswith(".sor"):
            sors.append((base, z.read(info)))
            continue
        if not low.endswith(IMG_EXT):
            continue
        pid = uuid.uuid4().hex[:12]
        src = os.path.join(tmp_dir, pid + os.path.splitext(low)[1])
        with open(src, "wb") as fh:
            fh.write(z.read(info))
        try:
            big, th, size, exif_time = save_variants(src, job_dir, pid)
        except Exception:
            os.remove(src)
            continue
        os.remove(src)
        group, sub = classify_path(rel, s1, s2)
        photos.append({"id": pid, "rel": rel, "name": base, "group": group, "sub": sub, "kind": None,
                       "path": big, "thumb": th, "w": size[0], "h": size[1], "exif_time": exif_time,
                       "feat": features(big), "lat": None, "lon": None})
    try:
        os.rmdir(tmp_dir)
    except OSError:
        pass
    photos.sort(key=photo_time_key)
    return photos, kmz_pts, sors, folders


def place_unknown(photos, kmz_pts, s1, s2):
    """รูปที่ไม่ได้อยู่ในโฟลเดอร์ที่รู้จัก: อ่านพิกัดบนรูป แล้วหาจุดใกล้สุดใน KMZ"""
    site_pts = {}
    for name, lat, lon in kmz_pts:
        n = name.lower().replace("_", "")
        if site_code(s1).lower() in n:
            site_pts[1] = (lat, lon)
        elif site_code(s2).lower() in n:
            site_pts[2] = (lat, lon)
    closure_pts = [(norm_closure(n), (la, lo)) for n, la, lo in kmz_pts if norm_closure(n)]
    for p in photos:
        if p["group"] != "unknown":
            continue
        ll = (p["lat"], p["lon"]) if p.get("lat") else ocr_latlon(p["path"])
        if not ll:
            continue
        p["lat"], p["lon"] = ll
        cands = [("closure", c, haversine(ll, pt)) for c, pt in closure_pts]
        cands += [("site", k, haversine(ll, pt)) for k, pt in site_pts.items()]
        if cands:
            g, sub, d = min(cands, key=lambda x: x[2])
            if d < 80:
                p["group"], p["sub"], p["gps_dist"] = g, sub, round(d)


def auto_assign(photos, closures):
    """วางรูปเข้าช่องตามกฎที่เรียนรู้จาก PAT ตัวอย่าง
    - ถ้ามีผล AI (p['kind']) ใช้ประเภทรูปจาก AI
    - ถ้าไม่มี ใช้การวิเคราะห์สี/ลำดับเวลา (แม่นน้อยกว่า -> ขึ้นให้คนตรวจ)"""
    assign, conf, method = {}, {}, {}
    ai = any(p.get("kind") for p in photos)

    def put(slot, p, c, how):
        if slot in assign or p is None:
            return
        assign[slot] = p["id"] if p != "NA" else "NA"
        conf[slot] = c
        method[slot] = how

    def by(g, s=None):
        return [p for p in photos if p["group"] == g and (s is None or p["sub"] == s) and p.get("kind") not in ("sign", "sfp", "other")]

    def pick(ps, kind, used=()):
        """เลือกรูปแรก (ตามเวลา) ที่ AI บอกว่าเป็นประเภทนี้"""
        c = [p for p in ps if p.get("kind") == kind and p["id"] not in used]
        if not c:
            return None, 0
        best = max(c, key=lambda p: float(p.get("kconf") or 0.8)) if kind == "site_location" else c[0]
        return best, float(best.get("kconf") or 0.85)

    # ---- หัวต่อ
    for cl in closures:
        ps = by("closure", cl)
        if ai and any(p.get("kind") for p in ps):
            used = set()
            for role, kind in (("front", "closure_front"), ("inside", "closure_inside"), ("pole", "closure_install")):
                p, c = pick(ps, kind, used)
                if p:
                    used.add(p["id"])
                    put(f"closure:{cl}:{role}", p, c, "ai")
        if not all(f"closure:{cl}:{r}" in assign for r in ("front", "inside", "pole")):
            free = [p for p in ps if p["id"] not in assign.values()]
            roles = assign_closure_roles(free)
            for idx, (role, c) in roles.items():
                put(f"closure:{cl}:{role}", free[idx], c, "folder")

    # ---- Site รอบที่ 1 (จากโฟลเดอร์ Site)
    for k in (1, 2):
        ps = by("site", k)
        if ai:
            used = set()
            for role, kind in (("location", "site_location"), ("odf", "odf_rack"), ("core", "core_label")):
                p, c = pick(ps, kind, used)
                if p:
                    used.add(p["id"])
                    put(f"site{k}:r1:{role}", p, c, "ai")
        if ps and f"site{k}:r1:location" not in assign:
            loc = max(ps[:6], key=lambda p: p["feat"]["sky"])
            put(f"site{k}:r1:location", loc, 0.8 if loc["feat"]["sky"] > .3 else .5, "folder")
        rest = [p for p in ps if p["id"] not in assign.values()]
        for role, p in zip(("odf", "core"), rest):
            put(f"site{k}:r1:{role}", p, 0.55, "folder")

    # ---- Plug Port (และ Site รอบที่ 2 = รูปชุดเดียวกับ Plug Port ตามตัวอย่าง)
    for k in (1, 2):
        ps = by("plug", k)
        if ai:
            used = set()
            for role, kind in (("location", "site_location"), ("port1", "equipment_port"), ("port2", "equipment_port"),
                               ("odf1", "odf_rack"), ("odf2", "core_label")):
                p, c = pick(ps, kind, used)
                if p:
                    used.add(p["id"])
                    put(f"plug{k}:{role}", p, c, "ai")
        elif ps:
            loc = max(ps, key=lambda p: p["feat"]["sky"])
            if loc["feat"]["sky"] > .3:
                put(f"plug{k}:location", loc, 0.7, "folder")
            rest = [p for p in ps if p["id"] not in assign.values()]
            for slot, p in zip(("port1", "port2", "odf1", "odf2"), rest):
                put(f"plug{k}:{slot}", p, 0.5, "folder")
        # ไม่มีรูป Location ในโฟลเดอร์ Plug Port -> ใช้รูป Location ของ Site รอบที่ 1 (ตามตัวอย่าง)
        if f"plug{k}:location" not in assign and f"site{k}:r1:location" in assign:
            assign[f"plug{k}:location"] = assign[f"site{k}:r1:location"]
            conf[f"plug{k}:location"], method[f"plug{k}:location"] = conf[f"site{k}:r1:location"], "rule"
        put(f"plug{k}:odf3", "NA", 1.0, "rule")
        for src, dst in (("location", "location"), ("odf1", "odf"), ("odf2", "core")):
            s_ = f"plug{k}:{src}"
            if s_ in assign and f"site{k}:r2:{dst}" not in assign:
                assign[f"site{k}:r2:{dst}"] = assign[s_]
                conf[f"site{k}:r2:{dst}"], method[f"site{k}:r2:{dst}"] = conf[s_], "rule"

    # ---- Power: รูปเครื่องวัด 2 รูป ช่องที่ 3 = N/A
    pw = by("power")
    if not ai:
        for p in pw:
            p.setdefault("kind", None)
            if p["kind"] is None:
                p["kind"] = guess_screen(p["path"])
    pw = [p for p in pw if p.get("kind") in (None, "power_meter")]
    for i, p in enumerate(pw[:2], 1):
        put(f"power:{i}", p, 0.9 if ai else 0.7, "ai" if ai else "folder")
    put("power:3", "NA", 1.0, "rule")

    d = by("design")
    if d:
        put("design", d[0], 1.0, "folder")
    pl = by("planwork")
    if pl:
        put("planwork", pl[-1], 0.8, "folder")
    return assign, conf, method


def guess_screen(path):
    """แยกรูปหน้าจอ Power Meter กับหน้าจอ OTDR ด้วย OCR (ใช้ตอนไม่มี AI)"""
    if pytesseract is None:
        return None
    try:
        im = Image.open(path).convert("L")
        t = pytesseract.image_to_string(im, lang="eng").upper()
    except Exception:
        return None
    if "POWER METER" in t or "DBM" in t:
        return "power_meter"
    if any(k in t for k in ("CUMUL", "ATT.", "FILE NAME", "EVENT", "TRACE", "FIBER LEN")):
        return "otdr_screen"
    return None


def fiber_length_from_images(photos):
    """อ่าน 'Fiber Lenght: x.xxx km' จากรูปในโฟลเดอร์ OTDR (OCR)"""
    if pytesseract is None:
        return None
    for p in photos:
        if p["group"] != "otdr" and p.get("kind") != "otdr_screen":
            continue
        try:
            t = pytesseract.image_to_string(Image.open(p["path"]).convert("L"), lang="eng")
        except Exception:
            continue
        m = re.search(r"Fiber\s*Len\w*\s*[:=]?\s*([\d.,]+)\s*km", t, re.I)
        if m:
            return float(m.group(1).replace(",", "."))
    return None


def run_import(zip_bytes, job_dir, s1, s2):
    photos, kmz_pts, sors, folders = extract_zip(zip_bytes, job_dir, s1, s2)
    place_unknown(photos, kmz_pts, s1, s2)
    closures = [norm_closure(n) for n, _, _ in kmz_pts if norm_closure(n)]
    for c in folders:
        if c not in closures:
            closures.append(c)
    for p in photos:
        if p["feat"]["sharp"] < 300:
            p["blurry"] = True
    return {"photos": photos, "kmz": kmz_pts, "closures": closures, "closure_folders": folders, "sors": sors}
