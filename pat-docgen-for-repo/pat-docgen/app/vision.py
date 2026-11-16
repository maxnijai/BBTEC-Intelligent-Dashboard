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


TOOL = {
    "name": "report_photos",
    "description": "รายงานประเภทของรูปเป้าหมายทุกรูป (ต้องมีครบทุกหมายเลข)",
    "input_schema": {"type": "object", "required": ["items"], "properties": {"items": {"type": "array", "items": {
        "type": "object", "required": ["n", "kind", "conf"], "properties": {
            "n": {"type": "integer", "description": "หมายเลขรูปเป้าหมาย"},
            "kind": {"type": "string", "enum": list(KINDS)},
            "conf": {"type": "number"},
            "lat": {"type": ["number", "null"]}, "lon": {"type": ["number", "null"]},
            "taken": {"type": ["string", "null"]}}}}}},
}


def _post(body):
    req = urllib.request.Request(API_URL, data=json.dumps(body).encode(), method="POST", headers={
        "x-api-key": os.environ["ANTHROPIC_API_KEY"], "anthropic-version": "2023-06-01", "content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=180) as r:
        return json.loads(r.read())


def _call(content, max_tokens=1500):
    """ถามแบบข้อความธรรมดา"""
    data = _post({"model": MODEL, "max_tokens": max_tokens, "messages": [{"role": "user", "content": content}]})
    text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
    u = data.get("usage", {})
    return text, u.get("input_tokens", 0), u.get("output_tokens", 0)


def _call_tool(content, max_tokens=4000):
    """บังคับให้ตอบผ่าน tool (ได้ JSON ที่มีโครงสร้างแน่นอน) คืน (items, in, out, stop_reason)"""
    data = _post({"model": MODEL, "max_tokens": max_tokens, "tools": [TOOL],
                  "tool_choice": {"type": "tool", "name": TOOL["name"]},
                  "messages": [{"role": "user", "content": content}]})
    items = []
    for b in data.get("content", []):
        if b.get("type") == "tool_use":
            items = (b.get("input") or {}).get("items") or []
        elif b.get("type") == "text" and not items:
            items = _parse_json(b.get("text", ""))
    u = data.get("usage", {})
    return items, u.get("input_tokens", 0), u.get("output_tokens", 0), data.get("stop_reason")


def _parse_json(text):
    m = re.search(r"\[.*\]", text or "", re.S)
    if not m:
        return []
    try:
        v = json.loads(m.group(0))
        return v if isinstance(v, list) else []
    except ValueError:
        return []


def _norm_kind(k):
    k = str(k or "").strip().lower().replace(" ", "_").replace("-", "_")
    return k if k in KINDS else None


def classify(photos, examples=None, batch=6):
    """photos: [(id, path)] หรือ [(id, path, folder)]  examples: [(kind, path)]
    คืน (results{id: {kind, conf, lat, lon, taken}}, usage{in, out, usd, errors})"""
    examples = examples or []
    LAST_ERRORS.clear()
    ex_content = []
    if examples:
        ex_content.append({"type": "text", "text": "รูปตัวอย่างที่คนจัดไว้แล้ว (ใช้เทียบเท่านั้น ไม่ต้องรายงานรูปเหล่านี้):"})
        for kind, path in examples[:33]:
            try:
                ex_content.append({"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": _b64(path, 320)}})
                note = " (รูปแบบนี้ไม่ใช้ในเอกสาร)" if kind in ("other", "sign", "sfp", "otdr_screen") else ""
                ex_content.append({"type": "text", "text": f"↑ ตัวอย่างประเภท {kind}{note}"})
            except OSError:
                continue
        if len(ex_content) > 1:  # ให้ระบบ cache ชุดตัวอย่าง (ลดค่าใช้จ่ายของชุดถัดไป)
            ex_content[-1]["cache_control"] = {"type": "ephemeral"}
    kinds_txt = "\n".join(f"- {k}: {v}" for k, v in KINDS.items())
    instr = (
        "คุณคือผู้ช่วยจัดรูปเอกสารส่งงานติดตั้งสาย Optical Fiber\n"
        f"ประเภทรูป:\n{kinds_txt}\n\n"
        "ต่อไปนี้คือ 'รูปเป้าหมาย' แต่ละรูปมีหมายเลขกำกับ ให้เรียก report_photos โดยมีรายการครบทุกหมายเลข "
        "(n = หมายเลขรูป, kind = ประเภท, conf = ความมั่นใจ 0-1 ตามจริง)\n"
        "lat/lon/taken ให้อ่านจากตัวหนังสือที่แอปกล้องพิมพ์ไว้บนรูป (มักเป็นสีส้มหรือสีขาว มุมขวาบน/ขวาล่าง) ถ้าอ่านไม่ได้ให้ใส่ null\n"
        "ชื่อโฟลเดอร์เป็นแค่คำใบ้: โฟลเดอร์ชื่อสถานี (เช่น PHE1149) มักมีรูป site_location/odf_rack/core_label, "
        "โฟลเดอร์ PlugPort มักมี equipment_port/odf_rack/core_label, โฟลเดอร์ SJ#/BJ# เป็นรูป closure, โฟลเดอร์ Power เป็น power_meter หรือ otdr_screen\n"
        "ถ้ามีรูปตัวอย่าง ให้เทียบกับรูปตัวอย่างเป็นหลัก: รูปตัวอย่างคือสิ่งที่ผู้ใช้บริษัทนี้จัดไว้จริงในงานก่อนหน้า "
        "ให้เลือกประเภทที่มุมกล้อง/วัตถุ/ระยะใกล้เคียงกับตัวอย่างที่สุด และรูปที่คล้ายตัวอย่างซึ่งไม่ใช้ในเอกสารให้ตอบประเภทนั้น"
    )
    results = {}
    stats = {"in": 0, "out": 0, "truncated": 0}

    def run(chunk):
        """คืน dict ผลของ chunk นี้ (key = id)"""
        content = list(ex_content) + [{"type": "text", "text": instr}]
        for n, item in enumerate(chunk, 1):
            folder = item[2] if len(item) > 2 else ""
            content.append({"type": "text", "text": f"รูปเป้าหมายหมายเลข {n} (โฟลเดอร์: {folder})"})
            content.append({"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": _b64(item[1], 768)}})
        items, i, o, stop = _call_tool(content)
        out = {}
        for r in items:
            if not isinstance(r, dict):
                continue
            try:
                n = int(r.get("n"))
            except (TypeError, ValueError):
                continue
            kind = _norm_kind(r.get("kind"))
            if kind and 1 <= n <= len(chunk):
                out[chunk[n - 1][0]] = {"id": chunk[n - 1][0], "kind": kind, "conf": r.get("conf"), "lat": r.get("lat"),
                                        "lon": r.get("lon"), "taken": r.get("taken")}
        return out, i, o, stop

    def run_all(chunks):
        with ThreadPoolExecutor(max_workers=3) as ex:
            for chunk, got in zip(chunks, ex.map(lambda c: _safe(run, c), chunks)):
                if not got:
                    continue
                out, i, o, stop = got
                stats["in"] += i
                stats["out"] += o
                if stop == "max_tokens":
                    stats["truncated"] += 1
                results.update(out)

    photos = list(photos)
    run_all([photos[i:i + batch] for i in range(0, len(photos), batch)])
    # รูปที่ยังไม่ได้ผล: ลองอีกรอบเป็นชุดเล็ก
    missing = [p for p in photos if p[0] not in results]
    if missing and len(missing) < len(photos):
        run_all([missing[i:i + 3] for i in range(0, len(missing), 3)])
    elif missing and not LAST_ERRORS:
        run_all([missing[i:i + 3] for i in range(0, min(len(missing), 6), 3)])  # ลองตัวอย่างเล็ก ๆ เพื่อเก็บสาเหตุ
    missing = [p for p in photos if p[0] not in results]
    errors = sorted(set(LAST_ERRORS))
    if missing:
        errors.append(f"AI ไม่ได้ตอบ {len(missing)} จาก {len(photos)} รูป" + (" (คำตอบถูกตัดเพราะยาวเกิน)" if stats["truncated"] else ""))
    usd = stats["in"] / 1e6 * PRICE_IN + stats["out"] / 1e6 * PRICE_OUT
    return results, {"in": stats["in"], "out": stats["out"], "usd": round(usd, 4), "errors": errors}


def read_text(path, question, side=1600):
    """อ่านข้อความเฉพาะจุดจากรูป (เช่น Fiber Lenght, พิกัด)"""
    content = [{"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": _b64(path, side)}},
               {"type": "text", "text": question}]
    out = _safe(lambda c: _call(c, 300), content)
    return out[0] if out else ""


LAST_ERRORS = []


def _explain(e):
    if isinstance(e, urllib.error.HTTPError):
        try:
            body = json.loads(e.read().decode())
            msg = body.get("error", {}).get("message", "")
        except Exception:
            msg = ""
        hint = {401: "API key ไม่ถูกต้อง", 403: "API key ไม่มีสิทธิ์ใช้งาน", 404: f"ไม่พบโมเดล {MODEL}",
                429: "เรียกถี่เกินไป/โควตาเต็ม", 400: "คำขอไม่ถูกต้อง (อาจเครดิตไม่พอ)", 529: "ระบบ AI ไม่ว่างชั่วคราว"}.get(e.code, "")
        return f"HTTP {e.code} {hint} {msg}".strip()
    return str(e)


def _safe(fn, arg):
    try:
        return fn(arg)
    except Exception as e:  # pragma: no cover
        LAST_ERRORS.append(_explain(e))
        print("vision error:", LAST_ERRORS[-1])
        return None
