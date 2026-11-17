"""ส่วนแสดงผล: แปลง Audit log / ชื่อช่อง ให้คนอ่านง่าย และย่อรายการที่ซ้ำ ๆ"""
import re

ACTIONS = {  # action: (ป้าย, โทนสี)
    "job_create": ("สร้างงาน", "blue"), "import": ("Import รูป", "blue"), "fields": ("แก้ข้อมูล", "gray"),
    "assign": ("จัดรูป", "gray"), "swap": ("จัดรูป", "gray"), "confirm": ("ยืนยันรูป", "gray"), "teach": ("สอนระบบ", "violet"),
    "ai_redo": ("จัดรูปใหม่", "violet"), "attach": ("แนบไฟล์", "gray"), "detach": ("ลบไฟล์แนบ", "red"),
    "sheet_cfg": ("ตั้งค่าหน้า Sheet", "gray"), "signature": ("ผู้ลงนาม", "gray"), "coords": ("พิกัด", "gray"),
    "kmz": ("ดาวน์โหลด KMZ", "gray"), "generate": ("สร้าง PDF", "green"), "download": ("ดาวน์โหลด", "gray"),
    "submit": ("ส่งตรวจ", "amber"), "approve": ("อนุมัติ", "green"), "reject": ("ตีกลับ", "red"), "reopen": ("เปิดแก้ไข", "amber"),
    "delete": ("ลบงาน", "red"), "doc_delete": ("ลบ PDF", "red"), "photo_delete": ("ลบรูป", "red"),
    "examples_clear": ("ล้างตัวอย่าง", "red"), "audit_clear": ("ล้างประวัติ", "red"), "siglib_delete": ("ลบลายเซ็น", "red"),
    "login": ("เข้าสู่ระบบ", "gray"), "logout": ("ออกจากระบบ", "gray"), "profile": ("โปรไฟล์", "gray"),
    "user_add": ("เพิ่มผู้ใช้", "blue"), "user_toggle": ("ผู้ใช้", "gray"), "user_reset": ("ตั้งรหัสผ่าน", "gray"), "company": ("บริษัท", "gray"),
}
NOISE = ("login", "logout", "download", "kmz")       # ซ่อนในมุมมองปกติ
MERGE = ("assign", "swap")                              # รวมรายการติดกันเป็นบรรทัดเดียว
ROLE_TH = {"superadmin": "ผู้ดูแลระบบ (ทุกบริษัท)", "admin": "Admin บริษัท", "editor": "ผู้จัดทำเอกสาร", "viewer": "ดูอย่างเดียว"}
_PART = {"location": "Location", "odf": "ตู้ ODF", "core": "Core", "port1": "พอร์ต 1", "port2": "พอร์ต 2", "odf1": "ODF 1",
         "odf2": "ODF 2", "odf3": "ช่องที่ 3", "front": "ด้านหน้า", "inside": "ด้านใน", "pole": "จุดติดตั้ง"}
_SLOT = r"(?:site\d:r\d:\w+|plug\d:\w+|closure:[^:\s]+:\w+|power:\d|design|planwork)"


def slot_label(slot):
    p = (slot or "").split(":")
    try:
        if p[0].startswith("site"):
            return f"Site {p[0][4:]} รอบ {p[1][1:]} · {_PART.get(p[2], p[2])}"
        if p[0].startswith("plug"):
            return f"Plug Port Site {p[0][4:]} · {_PART.get(p[1], p[1])}"
        if p[0] == "closure":
            return f"หัวต่อ {p[1]} · {_PART.get(p[2], p[2])}"
        if p[0] == "power":
            return f"Test Power {p[1]}"
    except IndexError:
        pass
    return {"design": "Topo Design", "planwork": "Support Planwork"}.get(slot, slot)


def nice(action, detail):
    """ข้อความ log ที่อ่านง่าย (รองรับ log เก่าที่เก็บรหัสรูป)"""
    d = detail or ""
    if action == "assign":
        m = re.match(rf"({_SLOT}): (\S+) → (\S+)", d)
        if m:
            lab = slot_label(m.group(1))
            return {"ว่าง": f"เอารูปออกจากช่อง {lab}", "NA": f"ตั้งช่อง {lab} เป็น N/A"}.get(m.group(3), f"เปลี่ยนรูปช่อง {lab}")
    if action == "swap":
        m = re.match(rf"ย้ายรูป ({_SLOT}) ⇄ ({_SLOT})", d)
        if m:
            return f"สลับรูป {slot_label(m.group(1))} ⇄ {slot_label(m.group(2))}"
    if action in ("import", "ai_redo"):
        d = re.sub(r" \([\d,]+ tokens ≈ \$[\d.]+\)", "", d)
        d = re.sub(r", ไม่ทราบกลุ่ม 0 รูป|, \.sor 0 ไฟล์", "", d)
    return d


def timeline(rows, show_all=False):
    """rows: Audit เรียงใหม่→เก่า  คืนรายการสำหรับแสดงผล (ซ่อน noise, รวมการจัดรูปที่ติดกัน)"""
    out = []
    for r in rows:
        if not show_all and r.action in NOISE:
            continue
        label, tone = ACTIONS.get(r.action, (r.action, "gray"))
        if not show_all and r.action in MERGE and out and out[-1]["merge"] and out[-1]["user_id"] == r.user_id \
                and out[-1]["job_id"] == r.job_id:
            out[-1]["n"] += 1
            out[-1]["text"] = f"จัดรูป {out[-1]['n']} ช่อง"
            out[-1]["first"] = r.created_at
            continue
        out.append({"when": r.created_at, "first": r.created_at, "user": r.user.name if r.user else "-", "user_id": r.user_id,
                    "job_id": r.job_id, "action": r.action, "label": label, "tone": tone, "text": nice(r.action, r.detail),
                    "merge": r.action in MERGE, "n": 1})
    return out


def check_target(msg):
    """ลิงก์ของรายการตรวจก่อนสร้าง -> ส่วนที่ต้องไปแก้"""
    if "Google Sheet" in msg or ".sor" in msg or "As-Built" in msg:
        return "files"
    if "ผู้ลงนาม" in msg:
        return "sign"
    if "พิกัด" in msg:
        return "kmz"
    if "Link Budget" in msg:
        return "data"
    if "รูป" in msg or "ช่อง" in msg:
        return "photos"
    return ""
