"""อ่านไฟล์ OTDR .sor (Bellcore SR-4731) -> dict ที่ใช้ทำหน้า Report OTDR และ Link Budget"""
import os
import re
import tempfile
from datetime import datetime, timedelta, timezone

from pyotdr import sorparse

TZ = timezone(timedelta(hours=7))


def _f(v, default=None):
    try:
        return float(str(v).split()[0])
    except (TypeError, ValueError, IndexError):
        return default


def parse_sor(data, filename="trace.sor"):
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, os.path.basename(filename) or "trace.sor")
        with open(p, "wb") as fh:
            fh.write(data)
        cwd = os.getcwd()
        try:
            os.chdir(d)  # pyotdr อาจเขียนไฟล์ข้าง ๆ
            status, res, trace = sorparse(p)
        finally:
            os.chdir(cwd)
    if status != "ok":
        raise ValueError(f"อ่านไฟล์ .sor ไม่ได้: {status}")
    gen, sup, fx, ke = res.get("GenParams", {}), res.get("SupParams", {}), res.get("FxdParams", {}), res.get("KeyEvents", {})
    summ = ke.get("Summary", {}) or {}
    n = int(ke.get("num events", 0) or 0)
    events = []
    for i in range(1, n + 1):
        e = ke.get(f"event {i}") or {}
        dist = _f(e.get("distance"), 0.0)
        loss = _f(e.get("splice loss"), 0.0)
        refl = _f(e.get("refl loss"), 0.0)
        slope = _f(e.get("slope"), 0.0)
        code = (e.get("type") or "")[:2]
        if i == 1:
            typ = "Begin"
        elif i == n or code[1:2] == "E":
            typ = "End"
        elif code[:1] == "1":
            typ = "Reflection"
        else:
            typ = "Gain" if loss < 0 else "Loss"
        events.append({"no": i, "type": typ, "distance": dist, "loss": loss, "refl": refl, "slope": slope})
    pts = []
    for line in trace:
        parts = line.split()
        if len(parts) >= 2:
            pts.append((float(parts[0]), float(parts[1])))
    # ลดจำนวนจุดสำหรับวาดกราฟ (เก็บ min/max ในแต่ละช่วงเพื่อคงรูปทรง noise)
    step = max(1, len(pts) // 900)
    slim = []
    for i in range(0, len(pts), step):
        chunk = pts[i:i + step]
        lo = min(chunk, key=lambda p: p[1])
        hi = max(chunk, key=lambda p: p[1])
        slim.extend(sorted({lo, hi}))
    length = summ.get("loss end") or (events[-1]["distance"] if events else None)
    length = round(float(length), 3) if length is not None else None
    total = _f(summ.get("total loss"))
    m = re.search(r"\((\d+) sec\)", str(fx.get("date/time", "")))
    dt = datetime.fromtimestamp(int(m.group(1)), TZ) if m else None
    wl = gen.get("wavelength") or fx.get("wavelength") or ""
    return {
        "file": os.path.basename(filename),
        "date": dt.strftime("%d-%b-%y %H:%M:%S") if dt else "",
        "date_iso": dt.isoformat() if dt else "",
        "fiber": (gen.get("fiber ID") or "").strip(),
        "cable": (gen.get("cable ID") or "").strip(),
        "begin": (gen.get("location A") or "").strip(),
        "end": (gen.get("location B") or "").strip(),
        "comment": (gen.get("comments") or "").strip(),
        "wavelength": wl.replace("nm", "").strip(),
        "pulse": fx.get("pulse width", ""),
        "index": _f(fx.get("index")),
        "range": _f(fx.get("range")),
        "loss_thr": fx.get("loss thr", ""),
        "refl_thr": fx.get("refl thr", ""),
        "otdr": (sup.get("OTDR") or "").strip(),
        "otdr_sn": (sup.get("OTDR S/N") or "").strip(),
        "module": (sup.get("module") or "").strip(),
        "module_sn": (sup.get("module S/N") or "").strip(),
        "length_km": length,
        "total_loss": total,
        "atten": round(total / length, 3) if total is not None and length else None,
        "events": events,
        "trace": slim,
    }
