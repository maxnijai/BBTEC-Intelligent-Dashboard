"""
ให้ Claude (Vision) ดูรูปแล้วบอกว่าเป็นรูปประเภทไหน + อ่านวันที่/พิกัดที่พิมพ์บนรูป
เปิดใช้เมื่อมี Variable ANTHROPIC_API_KEY เท่านั้น (ไม่มี = ระบบใช้กฎ/วิเคราะห์สีแทน)

ส่งครั้งละหลายรูป (batch) และแนบ "รูปตัวอย่าง" ที่ระบบเรียนรู้จากงานที่คนจัดไว้แล้ว (few-shot)
"""
import base64
import io
import json
import os
import re
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from PIL import Image

API_URL = "https://api.anthropic.com/v1/messages"
MODEL = os.environ.get("CLAUDE_MODEL", "claude-haiku-5-5")
PRICE_IN, PRICE_OUT = 0.10, 0.50  # USD ต่อ 1 ล้าน token (Haiku 5.5, prompt < 100K)

KINDS = {
    "site_location": "ภาพมุมกว้างกลางแจ้งของสถานี: เสา/ทาวเวอร์, shelter, รั้ว, ตู้อุปกรณ์ด้านนอก",
    "odf_rack": "ตู้/แร็คในอาคารที่เห็นแผง ODF หรือ patch panel และสาย fiber สีเหลือง (มุมกว้างถึงกลาง)",
    "core_label": "ภาพใกล้ของสาย fiber/หัว connector ที่มีป้ายกระดาษเขียนเลข Core หรือชื่อเส้นทาง มักถือด้วยมือ",
    "equipment_port": "พอร์ตของอุปกรณ์ Router/Switch/CPE (เช่น Huawei CX600) ที่เสียบสาย fiber",
    "power_meter": "หน้าจอเครื่องวัด Optical Power Meter แสดงค่า dBm",
    "otdr_screen": "หน้าจอเครื่อง OTDR หรือรูป/รายงานผล OTDR ที่มีกราฟ trace",
    "closure_front": "กล่องต่อสาย (splice closure) ที่ปิดฝาแล้ว ถ่ายด้านหน้า/ป้ายบนกล่อง",
    "closure_inside": "ภายในกล่องต่อสายที่เปิดฝา เห็นถาด splice และ fiber ที่ต่อเสร็จ",
    "closure_install": "จุดติดตั้ง closure มุมกว้าง เช่น บนเสาไฟฟ้า/บนสาย เห็นท้องฟ้าหรือถนน",
    "sign": "ป้ายเตือน/ป้ายชื่อสถานี (ไม่ใช้ในเอกสาร)",
    "sfp": "โมดูล SFP/transceiver ถือในมือ (ไม่ใช้ในเอกสาร)",
    "other": "อื่น ๆ",
}


def enabled():
    return bool(os.environ.get("ANTHROPIC_API_KEY"))


def _b64(path, side):
    im = Image.open(path).convert("RGB")
    im.thumbnail((side, side))
    buf = io.BytesIO()
    im.save(buf, "JPEG", quality=72)
    return base64.b64encode(buf.getvalue()).decode()


def _call(content, max_tokens=1500):
    body = json.dumps({"model": MODEL, "max_tokens": max_tokens, "messages": [{"role": "user", "content": content}]}).encode()
    req = urllib.request.Request(API_URL, data=body, method="POST", headers={
        "x-api-key": os.environ["ANTHROPIC_API_KEY"], "anthropic-version": "2023-06-01", "content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        data = json.loads(r.read())
    text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
    u = data.get("usage", {})
    return text, u.get("input_tokens", 0), u.get("output_tokens", 0)


def _parse_json(text):
    m = re.search(r"\[.*\]", text, re.S)
    return json.loads(m.group(0)) if m else []


def classify(photos, examples=None, batch=8):
    """photos: [(id, path)]  examples: [(kind, path)]
    คืน (results{id: {kind, conf, lat, lon, taken}}, usage{in, out, usd})"""
    examples = examples or []
    ex_content = []
    if examples:
        ex_content.append({"type": "text", "text": "รูปตัวอย่างที่คนจัดไว้แล้ว (ใช้เทียบ):"})
        for kind, path in examples[:16]:
            try:
                ex_content.append({"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": _b64(path, 256)}})
                ex_content.append({"type": "text", "text": f"↑ ตัวอย่างประเภท {kind}"})
            except OSError:
                continue
    kinds_txt = "\n".join(f"- {k}: {v}" for k, v in KINDS.items())
    instr = (
        "คุณคือผู้ช่วยจัดรูปเอกสารส่งงานติดตั้งสาย Optical Fiber\n"
        f"ประเภทรูป:\n{kinds_txt}\n\n"
        "สำหรับรูปเป้าหมายแต่ละรูป (ระบุด้วย id) ให้ตอบ JSON array เท่านั้น เช่น "
        '[{"id":"..","kind":"odf_rack","conf":0.9,"lat":17.97,"lon":100.05,"taken":"2026-09-10 11:40"}]\n'
        "lat/lon/taken ให้อ่านจากตัวหนังสือที่แอปกล้องพิมพ์ไว้บนรูป (มักเป็นสีส้มหรือสีขาว มุมขวาบน/ขวาล่าง) ถ้าอ่านไม่ได้ให้ใส่ null"
    )
    results, tin, tout = {}, 0, 0

    def run(chunk):
        content = list(ex_content) + [{"type": "text", "text": instr}]
        for pid, path in chunk:
            content.append({"type": "text", "text": f"id={pid}"})
            content.append({"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": _b64(path, 768)}})
        return _call(content)

    chunks = [photos[i:i + batch] for i in range(0, len(photos), batch)]
    with ThreadPoolExecutor(max_workers=4) as ex:
        for out in ex.map(lambda c: _safe(run, c), chunks):
            if not out:
                continue
            text, i, o = out
            tin += i
            tout += o
            for r in _parse_json(text):
                if isinstance(r, dict) and r.get("id") and r.get("kind") in KINDS:
                    results[str(r["id"])] = r
    usd = tin / 1e6 * PRICE_IN + tout / 1e6 * PRICE_OUT
    return results, {"in": tin, "out": tout, "usd": round(usd, 4)}


def read_text(path, question, side=1600):
    """อ่านข้อความเฉพาะจุดจากรูป (เช่น Fiber Lenght, พิกัด)"""
    content = [{"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": _b64(path, side)}},
               {"type": "text", "text": question}]
    out = _safe(lambda c: _call(c, 300), content)
    return out[0] if out else ""


def _safe(fn, arg):
    try:
        return fn(arg)
    except (urllib.error.URLError, OSError, ValueError, KeyError) as e:  # pragma: no cover
        print("vision error:", e)
        return None
