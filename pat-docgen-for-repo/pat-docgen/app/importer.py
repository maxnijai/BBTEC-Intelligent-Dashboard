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


def open_image(path):
    im = Image.open(path)
    im = ImageOps.exif_transpose(im)
    return im.convert("RGB")


def save_variants(src_path, out_dir, stem):
    """สร้างรูปสำหรับ PDF (ยาวสุด 1400px) และ thumbnail 360px"""
    im = open_image(src_path)
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


def ocr_latlon(path):
    """อ่านพิกัดที่แอปกล้องพิมพ์ไว้บนรูป (มุมขวาบน หรือ ขวาล่าง)"""
    if pytesseract is None:
        return None
    im = Image.open(path).convert("RGB")
    w, h = im.size
    for box in ((int(w * .35), 0, w, int(h * .2)), (int(w * .25), int(h * .78), w, h)):
        c = np.asarray(im.crop(box)).astype(int)
        r, g, b = c[..., 0], c[..., 1], c[..., 2]
        for mk in ((r > 170) & (r - b > 70) & (g > 80), (r > 200) & (g > 200) & (b > 200), (r > 150) & (r - b > 50)):
            bw = Image.fromarray(np.where(mk, 0, 255).astype("uint8"))
            bw = bw.resize((bw.width * 2, bw.height * 2)).filter(ImageFilter.MedianFilter(3))
            try:
                t = pytesseract.image_to_string(bw, lang="eng", config="--psm 6 -c tessedit_char_whitelist=0123456789.,+:-/ ")
            except Exception:
                return None
            m = GPS_RX.search(t)
            if m:
                lat = float(m.group(1) + "." + m.group(2))
                lon = float(m.group(3) + "." + m.group(4))
                if 5 < lat < 21 and 97 < lon < 106:
                    return lat, lon
    return None

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
    conf = 0.6 + min(0.39, margin * 1.5)
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


# ---------------------------------------------------------------- main import


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
    if "POWER" in joined or "OPM" in joined:
        return ("power", None)
    if site:
        return ("site", site)
    return ("unknown", None)


def run_import(zip_bytes, job_dir, s1, s2, sort_key=None):
    """แตก ZIP, ทำรูปย่อ, จับคู่ช่อง  คืนค่า dict(photos=[...], kmz=[...], assign={slot: photo_id}, review=[slot...])"""
    os.makedirs(job_dir, exist_ok=True)
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))
    photos, kmz_pts = [], []
    tmp_dir = os.path.join(job_dir, "_src")
    os.makedirs(tmp_dir, exist_ok=True)
    for info in z.infolist():
        if info.is_dir():
            continue
        rel = info.filename
        base = os.path.basename(rel)
        if base.startswith(".") or "__MACOSX" in rel:
            continue
        low = base.lower()
        if low.endswith(".kmz") or low.endswith(".kml"):
            kmz_pts = parse_kmz(z.read(info))
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
        photos.append({"id": pid, "rel": rel, "name": base, "group": group, "sub": sub,
                       "path": big, "thumb": th, "w": size[0], "h": size[1], "exif_time": exif_time,
                       "feat": features(big), "lat": None, "lon": None})
    try:
        os.rmdir(tmp_dir)
    except OSError:
        pass

    # OCR พิกัด เฉพาะรูปที่ไม่รู้กลุ่ม (เร็วกว่า) -> หาจุดที่ใกล้สุดใน KMZ
    site_pts = {}
    for name, lat, lon in kmz_pts:
        if site_code(s1).lower() in name.lower().replace("_", ""):
            site_pts[1] = (lat, lon)
        elif site_code(s2).lower() in name.lower().replace("_", ""):
            site_pts[2] = (lat, lon)
    closure_pts = [(norm_closure(n), (la, lo)) for n, la, lo in kmz_pts if norm_closure(n)]
    for p in photos:
        if p["group"] != "unknown":
            continue
        ll = ocr_latlon(p["path"])
        if not ll:
            continue
        p["lat"], p["lon"] = ll
        cands = [("closure", c, haversine(ll, pt)) for c, pt in closure_pts]
        cands += [("site", k, haversine(ll, pt)) for k, pt in site_pts.items()]
        if cands:
            g, sub, d = min(cands, key=lambda x: x[2])
            if d < 80:
                p["group"], p["sub"], p["gps_dist"] = g, sub, round(d)

    photos.sort(key=sort_key or photo_time_key)

    closures_detected = [c for c, _ in closure_pts]
    for p in photos:
        if p["group"] == "closure" and p["sub"] not in closures_detected:
            closures_detected.append(p["sub"])

    assign, conf, method = {}, {}, {}

    def put(slot, p, c, how):
        if slot not in assign and p is not None:
            assign[slot] = p["id"]
            conf[slot] = c
            method[slot] = how
            p.setdefault("used", 0)
            p["used"] += 1

    by = lambda g, s=None: [p for p in photos if p["group"] == g and (s is None or p["sub"] == s)]

    # หัวต่อ
    for cl in closures_detected:
        ps = by("closure", cl)
        roles = assign_closure_roles(ps)
        how = "gps" if any(p.get("gps_dist") is not None for p in ps) else "folder"
        for idx, (role, c) in roles.items():
            put(f"closure:{cl}:{role}", ps[idx], c, how)

    # Site รอบ 1 / รอบ 2 : เรียงตามเวลา, รูปที่ท้องฟ้าเยอะสุดในชุด = Location
    for k in (1, 2):
        ps = by("site", k)
        rounds = [ps[0:3], ps[3:6]]
        for r, chunk in enumerate(rounds, 1):
            if not chunk:
                continue
            chunk = list(chunk)
            loc = max(chunk, key=lambda p: p["feat"]["sky"])
            put(f"site{k}:r{r}:location", loc, 0.85, "folder")
            rest = [p for p in chunk if p is not loc]
            if rest:
                put(f"site{k}:r{r}:odf", rest[0], 0.7, "folder")
            if len(rest) > 1:
                put(f"site{k}:r{r}:core", rest[1], 0.7, "folder")

    # Power
    for i, p in enumerate(by("power")[:3], 1):
        put(f"power:{i}", p, 0.9, "folder")

    # Plug port
    for k in (1, 2):
        ps = by("plug", k)
        if not ps:
            continue
        loc = max(ps, key=lambda p: p["feat"]["sky"])
        put(f"plug{k}:location", loc, 0.7 if loc["feat"]["sky"] > 0.3 else 0.5, "folder")
        rest = [p for p in ps if p is not loc]
        for slot, p in zip([f"plug{k}:port1", f"plug{k}:port2", f"plug{k}:odf1", f"plug{k}:odf2"], rest):
            put(slot, p, 0.6, "folder")

    d = by("design")
    if d:
        put("design", d[0], 1.0, "folder")
    pw = by("planwork")
    if pw:
        put("planwork", pw[-1], 0.8, "folder")

    review = sorted(s for s, c in conf.items() if c < REVIEW_BELOW)
    for p in photos:
        if p["feat"]["sharp"] < 300:
            p["blurry"] = True
    return {"photos": photos, "kmz": kmz_pts, "closures": closures_detected,
            "assign": assign, "conf": conf, "method": method, "review": review}
