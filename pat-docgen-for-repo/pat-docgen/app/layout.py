"""
โครงหน้าเอกสาร PAT OFC v3.0 (ตามไฟล์ตัวอย่าง Rehome Site Dtac DN_PHE1149-D_PHE0020)

แต่ละหน้าเป็น dict:
  {"kind": "cover" | "sheet" | "photos" | "single" | "linkbudget" | "deficiency",
   "title": str, "slots": [ {key, caption} ... ]}

slot key ใช้เป็นตัวเชื่อมรูปกับช่องในเอกสาร เช่น
  site1:r1:location, site2:r2:odf, power:1, plug1:port1, closure:SJ#01:front
"""
import re

SITE_SLOTS = [("location", "Location Site"), ("odf", "ตำแหน่งติดตั้งตู้ ODF"), ("core", "Core ใช้งาน")]
CLOSURE_SLOTS = [("front", "รูปด้านหน้า closure"), ("inside", "รูปด้านใน closure ที่ตัดต่อเสร็จแล้ว"), ("pole", "จุดเก็บติดตั้ง closure")]

DEFAULT_FIELDS = {
    "project": "Rehoming Project",
    "region": "North Region",
    "activity": "Rehome Site Dtac",
    "sap_no": "",
    "company_display": "",
    "company_address": "",
    "ofc": "",
    "core": "",
    "or_no": "",
    "inspect_project": "Support Single Grid Project",
    "def_region": "",
    "equip1": "CX600",
    "equip2": "CPE",
    "power_core1": "",
    "power_core2": "",
    # link budget
    "lb_L": "",
    "lb_aF": "0.20",
    "lb_NE": "",
    "lb_aE": "0.15",
    "lb_NC": "2",
    "lb_aC": "0.50",
    "lb_wavelength": "1550",
    "lb_otdr_max": "4.60",   # ค่า Fix ตามลูกค้า
    "lb_otdr_min": "4.44",   # ค่า Fix ตามลูกค้า
    # ช่วงหน้าที่จะดึงจาก PDF ที่ export จาก Google Sheet (หน้า Check list / Loss / Material / Balance)
    "sheet_pages": "",
    "closures": "",   # "SJ#01,SJ#02,..." ว่าง = อ่านจาก KMZ / โฟลเดอร์
}

SITE_RX = re.compile(r"\b([A-Z]{1,3}\d?_[A-Z]{2,4}\d{3,5})\b")   # D_PHE0020, DN_PHE1149, PN2_LPN3052, T_LPN0078 ...


def sites_from_link(link_name):
    """'Rehome Site Dtac DN_PHE1149-D_PHE0020' -> ('DN_PHE1149', 'D_PHE0020')"""
    found = SITE_RX.findall(link_name or "")
    s1 = found[0] if found else "SITE1"
    s2 = found[1] if len(found) > 1 else "SITE2"
    return s1, s2


def site_code(site):
    """'DN_PHE1149' -> 'PHE1149' (ใช้จับคู่ชื่อโฟลเดอร์)"""
    return site.split("_", 1)[-1].upper()


def closure_list(job_fields, detected):
    raw = (job_fields.get("closures") or "").strip()
    if raw:
        return [c.strip().upper() for c in raw.split(",") if c.strip()]
    return detected or []


def build_pages(link_name, fields, closures):
    s1, s2 = sites_from_link(link_name)
    e1, e2 = fields.get("equip1") or "CX600", fields.get("equip2") or "CPE"
    pages = [{"kind": "cover", "title": "ปก PAT", "slots": []},
             {"kind": "checklist", "title": "รายการเอกสารส่งงานติดตั้ง Optical Fiber Cable", "slots": []},
             {"kind": "inspection", "title": "สรุปผลการตรวจรับการติดตั้ง OPTICAL FIBER CABLE", "slots": []},
             {"kind": "sheet", "title": "หน้าจาก Google Sheet (Loss OTDR, ใบสรุปเคเบิลรื้อถอน, Balance Cable & Material)", "slots": []}]
    for rnd in (1, 2):
        for idx, site in ((1, s1), (2, s2)):
            pages.append({
                "kind": "photos",
                "title": f"Site {site} (รอบที่ {rnd})",
                "slots": [{"key": f"site{idx}:r{rnd}:{k}", "caption": c} for k, c in SITE_SLOTS],
            })
    c1 = fields.get("power_core1") or "?"
    c2 = fields.get("power_core2") or "?"
    pages.append({"kind": "photos", "title": "Confirm Power", "slots": [
        {"key": "power:1", "caption": f"Test Power Core {c1} ({s1}-{s2})"},
        {"key": "power:2", "caption": f"Test Power Core {c2} ({s1}-{s2})"},
        {"key": "power:3", "caption": ""},
    ]})
    for idx, site, eq in ((1, s1, e1), (2, s2, e2)):
        pages.append({"kind": "photos", "title": f"Site {site} (Plug Port {eq})", "slots": [
            {"key": f"plug{idx}:location", "caption": "Location Site"},
            {"key": f"plug{idx}:port1", "caption": f"หลัง Plug Port {eq} {site}"},
            {"key": f"plug{idx}:port2", "caption": f"หลัง Plug Port {eq} {site}"},
        ]})
        pages.append({"kind": "photos", "title": f"Site {site} (Plug Port {eq})", "slots": [
            {"key": f"plug{idx}:odf1", "caption": f"หลัง Plug Core ODF {site}"},
            {"key": f"plug{idx}:odf2", "caption": f"หลัง Plug Core ODF {site}"},
            {"key": f"plug{idx}:odf3", "caption": ""},
        ]})
    pages.append({"kind": "linkbudget", "title": "Link Budget", "slots": []})
    for n, cl in enumerate(closures, 1):
        pages.append({"kind": "photos", "title": f"รูปภาพหัวต่อ หัวที่ {n} ({cl})",
                      "slots": [{"key": f"closure:{cl}:{k}", "caption": c} for k, c in CLOSURE_SLOTS]})
    pages.append({"kind": "deficiency", "title": "Deficiency List", "slots": []})
    pages.append({"kind": "single", "title": "Topo Design", "slots": [{"key": "design", "caption": ""}]})
    pages.append({"kind": "single", "title": "Support Planwork", "slots": [{"key": "planwork", "caption": ""}]})
    pages.append({"kind": "otdr", "title": "Report OTDR (จากไฟล์ .sor)", "slots": []})
    return pages


def all_slot_keys(pages):
    return [s["key"] for p in pages for s in p["slots"]]


OTDR_MAX, OTDR_MIN = 4.60, 4.44  # ค่า Fix ตามลูกค้า (แก้ในหน้างานได้ถ้าจำเป็น)


def link_budget(fields):
    def f(k):
        try:
            return float(str(fields.get(k, "")).replace(",", "."))
        except ValueError:
            return None
    L, aF, NE, aE, NC, aC = (f(k) for k in ("lb_L", "lb_aF", "lb_NE", "lb_aE", "lb_NC", "lb_aC"))
    out = {"L": L, "aF": aF, "NE": NE, "aE": aE, "NC": NC, "aC": aC,
           "otdr_max": f("lb_otdr_max") if f("lb_otdr_max") is not None else OTDR_MAX,
           "otdr_min": f("lb_otdr_min") if f("lb_otdr_min") is not None else OTDR_MIN}
    if None in (L, aF, NE, aE, NC, aC):
        out.update(cable=None, splice=None, conn=None, total=None)
        return out
    out["cable"] = round(L * aF, 2)
    out["splice"] = round(NE * aE, 2)
    out["conn"] = round(NC * aC, 2)
    out["total"] = round(out["cable"] + out["splice"] + out["conn"], 2)
    return out
