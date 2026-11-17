"""สร้างเล่ม PAT / AS-BUILT เป็น Excel (.xlsx) — 1 ชีต = 1 หน้า เรียงเหมือน PDF
- หน้ารูป, Topo Design, Support Planwork, Link Budget : เป็นเซลล์จริง แก้ข้อความ/เปลี่ยนรูปได้ (Link Budget มีสูตร)
- หน้าฟอร์ม (ปก, Checklist, สรุปผลตรวจรับ, Deficiency), หน้าจาก Google Sheet, แบบ As-Built, Report OTDR :
  วางเป็นภาพของหน้านั้น (หน้าตาตรงกับ PDF ทุกจุด)
"""
import io
import re

import xlsxwriter
from PIL import Image, ImageOps

from . import pdfgen
from .layout import sites_from_link, link_budget

try:
    import pypdfium2 as pdfium
except Exception:  # pragma: no cover
    pdfium = None

FONT = "TH Sarabun New"
PT = 96 / 72.0          # 1pt = 1.333px (Excel วางรูปเป็น px ที่ 96 dpi)
COLW = 5.0              # ความกว้างคอลัมน์มาตรฐานของชีต (หน่วยตัวอักษร) ~ 40px = 30pt
COLPT = 30.0


def _sheet_name(used, title):
    n = re.sub(r"[\[\]\:\*\?\/\\']", " ", title).strip()[:28] or "Sheet"
    base, i = n, 2
    while n.lower() in used:
        n = f"{base[:25]} ({i})"
        i += 1
    used.add(n.lower())
    return n


def _short(title):
    """ชื่อชีตสั้น ๆ (Excel จำกัด 31 ตัวอักษร)"""
    t = title
    m = re.match(r"รูปภาพหัวต่อ หัวที่ (\d+) \((.+)\)", t)
    if m:
        return f"หัวต่อ {m.group(1)} {m.group(2)}"
    t = re.sub(r"^Site ", "", t)
    t = re.sub(r"\(รอบที่ (\d)\)", r"รอบ\1", t)
    t = re.sub(r"\(Plug Port (.+)\)", r"Plug \1", t)
    return t


def _jpeg(path, w_pt, h_pt, stretch=True, scale=2.0):
    """ย่อรูปให้พอดีกรอบ (pt) คืน (BytesIO, กว้าง pt, สูง pt)"""
    im = ImageOps.exif_transpose(Image.open(path)).convert("RGB")
    if stretch:
        w, h = w_pt, h_pt
    else:
        r = min(w_pt / im.width, h_pt / im.height)
        w, h = im.width * r, im.height * r
    im = im.resize((max(1, int(w * PT * scale)), max(1, int(h * PT * scale))), Image.LANCZOS)
    b = io.BytesIO()
    im.save(b, "JPEG", quality=80, dpi=(96, 96))
    b.seek(0)
    return b, w, h, 1.0 / scale


def _logo(path, h_pt):
    im = Image.open(path)
    w_pt = h_pt * im.width / im.height
    b = io.BytesIO()
    im.convert("RGBA").save(b, "PNG", dpi=(96, 96))
    b.seek(0)
    return b, w_pt, (h_pt * PT) / im.height


def _pdf_images(pdf_bytes, indexes=None, dpi=130):
    """แปลงหน้า PDF เป็นรูป -> [(BytesIO, กว้าง pt, สูง pt)]"""
    if pdfium is None or not pdf_bytes:
        return []
    out = []
    doc = pdfium.PdfDocument(pdf_bytes)
    try:
        for i in (range(len(doc)) if indexes is None else indexes):
            pg = doc[i]
            w_pt, h_pt = pg.get_size()
            im = pg.render(scale=dpi / 72.0).to_pil().convert("RGB")
            b = io.BytesIO()
            im.save(b, "JPEG", quality=82, dpi=(96, 96))
            b.seek(0)
            out.append((b, w_pt, h_pt, (w_pt * PT) / im.width))
    finally:
        doc.close()
    return out


class Book:
    def __init__(self):
        self.buf = io.BytesIO()
        self.wb = xlsxwriter.Workbook(self.buf, {"in_memory": True})
        self.used = set()
        self.n = 0
        base = {"font_name": FONT, "font_size": 14, "valign": "vcenter"}
        f = lambda **k: self.wb.add_format({**base, **k})
        self.f = {
            "title": f(bold=True, align="center", font_size=16, border=2),
            "cell": f(align="center", left=2, right=2),
            "cap": f(bold=True, underline=True, align="center", font_size=14, left=2, right=2, bottom=6),
            "na": f(bold=True, align="center", font_size=26, left=2, right=2, bottom=6),
            "miss": f(bold=True, align="center", font_size=16, font_color="#C00000", left=2, right=2),
            "txt": f(), "b": f(bold=True), "r": f(align="right"), "c": f(align="center"),
            "u": f(bottom=1), "red": f(bold=True, font_color="#C00000", font_size=12),
            "head": f(bold=True, align="center", font_size=16, bg_color="#00FFFF", border=1),
            "box": f(align="center", border=1),
            "v": f(align="center", bottom=1, num_format="0.00"),
            "v3": f(align="center", bottom=1, num_format="0.000", bg_color="#00FFFF"),
            "vhl": f(align="center", bottom=1, num_format="0.00", bg_color="#00FFFF"),
            "vred": f(align="center", bottom=1, num_format="0.00", font_color="#C00000"),
            "vy": f(align="center", bottom=1, num_format="0.00", bg_color="#FFFF00", bold=True),
        }

    def sheet(self, title, landscape=False):
        self.n += 1
        ws = self.wb.add_worksheet(_sheet_name(self.used, f"{self.n:02d} {_short(title)}"))
        ws.hide_gridlines(2)
        ws.set_paper(1)  # Letter เหมือน Template
        if landscape:
            ws.set_landscape()
        ws.set_margins(0.4, 0.4, 0.4, 0.4)
        ws.center_horizontally()
        ws.fit_to_pages(1, 1)
        ws.set_column(0, 40, COLW)
        return ws

    # ---------------------------------------------------------- หน้ารูป 3 ช่อง
    def photos(self, page, by_slot, logo_l, logo_r):
        closure = bool(page["slots"]) and page["slots"][0]["key"].startswith("closure:")
        _, _, w, hh, rh, dx, dy, iw, ih, cdy, _ = pdfgen.GEOM["closure" if closure else "photos"]
        ncol = int(round(w / COLPT))
        ws = self.sheet(page["title"])
        c0, c1 = 1, ncol
        ws.set_row(1, hh)
        ws.merge_range(1, c0, 1, c1, page["title"], self.f["title"])
        self._logos(ws, 1, c0, c1, hh, logo_l, logo_r)
        caph = rh - cdy + 2
        r = 2
        for s in page["slots"]:
            p = by_slot.get(s["key"])
            ws.set_row(r, rh - caph)
            ws.set_row(r + 1, caph)
            if p == "NA" or (p is None and not s["caption"]):
                ws.merge_range(r, c0, r + 1, c1, "N/A", self.f["na"])
            else:
                ws.merge_range(r, c0, r, c1, "" if p else "ไม่มีรูป", self.f["cell"] if p else self.f["miss"])
                ws.merge_range(r + 1, c0, r + 1, c1, s["caption"], self.f["cap"])
                if p:
                    b, _, _, sc = _jpeg(p, iw, ih)
                    ws.insert_image(r, c0, "p.jpg", {"image_data": b, "x_scale": sc, "y_scale": sc, "object_position": 1,
                                                     "x_offset": (ncol * COLPT - iw) / 2 * PT, "y_offset": dy * PT})
            r += 2
        ws.print_area(0, 0, r, c1 + 1)

    def _logos(self, ws, row, c0, c1, hh, logo_l, logo_r):
        try:
            if logo_l:
                b, _, sc = _logo(logo_l, hh - 4)
                ws.insert_image(row, c0, "l.png", {"image_data": b, "x_scale": sc, "y_scale": sc, "x_offset": 3, "y_offset": 3})
            if logo_r:
                b, wpt, sc = _logo(logo_r, hh - 6)
                ws.insert_image(row, c1, "r.png", {"image_data": b, "x_scale": sc, "y_scale": sc,
                                                   "x_offset": max(0, (COLPT - wpt) * PT - 4), "y_offset": 4})
        except Exception:
            pass

    # ---------------------------------------------------------- หน้ารูปเดี่ยว (แนวนอน)
    def single(self, page, by_slot, logo_l, logo_r):
        w, h, hh = 669, 422, 27
        ncol = int(round(w / COLPT))
        ws = self.sheet(page["title"], landscape=True)
        ws.set_row(1, hh)
        ws.merge_range(1, 1, 1, ncol, page["title"], self.f["title"])
        self._logos(ws, 1, 1, ncol, hh, logo_l, logo_r)
        ws.set_row(2, h - hh)
        p = by_slot.get(page["slots"][0]["key"])
        ok = p and p != "NA"
        ws.merge_range(2, 1, 2, ncol, "" if ok else "N/A", self.wb.add_format(
            {"font_name": FONT, "bold": True, "font_size": 26, "align": "center", "valign": "vcenter", "border": 2}))
        if ok:
            b, iw, ih, sc = _jpeg(p, w - 50, h - hh - 40, stretch=False)
            ws.insert_image(2, 1, "p.jpg", {"image_data": b, "x_scale": sc, "y_scale": sc, "object_position": 1,
                                            "x_offset": (ncol * COLPT - iw) / 2 * PT, "y_offset": (h - hh - ih) / 2 * PT})
        ws.print_area(0, 0, 3, ncol + 1)

    # ---------------------------------------------------------- Link Budget (มีสูตร)
    def linkbudget(self, job):
        f = job["fields"]
        s1, s2 = sites_from_link(job["link_name"])
        lb = link_budget(f)
        ws = self.sheet("Link Budget")
        F = self.f
        ws.set_column(0, 0, 3)
        ws.set_column(1, 1, 62)
        ws.set_column(2, 2, 4)
        ws.set_column(3, 3, 16)
        ws.set_column(4, 4, 9)
        ws.set_column(5, 5, 24)
        ws.set_row(1, 34)
        ws.merge_range(1, 1, 1, 5, "8.2 OPTICAL FIBER LINK BUDGET", F["head"])
        for i, (lab, val) in enumerate((("Link : Name of station  A :", s2), ("Link : Name of station  B :", s1),
                                        ("Route Name :", f"{s2} - {s1}"))):
            ws.write(3 + i, 1, lab, F["r"])
            ws.merge_range(3 + i, 2, 3 + i, 5, val, F["u"])
        ws.merge_range(7, 1, 7, 5, "ค่า Attenuation โดยรวม (dB) = ( L x aF ) + ( NE X aE ) + ( NC X aC )", F["box"])

        def line(r, label, val, unit, fmt="v", note=""):
            ws.write(r, 1, label, F["txt"])
            ws.write(r, 2, "=", F["c"])
            if isinstance(val, str) and val.startswith("="):
                ws.write_formula(r, 3, val, F[fmt])
            elif val is None:
                ws.write_blank(r, 3, None, F[fmt])
            else:
                ws.write_number(r, 3, val, F[fmt])
            ws.write(r, 4, unit, F["txt"])
            if note:
                ws.write(r, 5, note, F["red"])
        line(9, "L  = ความยาวของ Optical Fiber", lb["L"], "Km.", "v3", "ระยะจาก OTDR")
        line(10, "aF = ค่า Attenuation ของ fiber", lb["aF"], "dB/Km.")
        line(11, "NE = จำนวนของจุด splice ไม่รวมใน ODF", lb["NE"], "Point", "vhl")
        line(12, "aE  = ค่าเฉลี่ยของ attenuation ของจุด Splice รวมกันทุกจุด", lb["aE"], "dB", "vred", "กำหนดค่า ใหม่เป็น 0.15")
        line(13, "NC = จำนวนของ Connector", lb["NC"], "Point", "vhl")
        line(14, "aC  = ค่าเฉลี่ยของ attenuation ต่อ connector รวม Splice ใน ODF", lb["aC"], "dB")
        line(16, "ค่า attenuation โดยรวมของ Cable ทั้งเส้นทาง (LxaF)", "=ROUND(D10*D11,2)", "dB")
        line(17, "ค่า attenuation โดยรวมของจุด Splice ทั้งเส้นทาง (NExaE)", "=ROUND(D12*D13,2)", "dB")
        line(18, "ค่า attenuation โดยรวมของ Connector ทั้งเส้นทาง (NCxaC)", "=ROUND(D14*D15,2)", "dB")
        line(20, "ค่า attenuation โดยรวมของเส้นทางโดยประมาณ (จากการคำนวณ)", "=ROUND(D17+D18+D19,2)", "dB", "vy")
        line(21, "ค่า attenuation โดยรวมของเส้นทางเมื่อทดสอบด้วยเครื่อง OTDR (Max.)", lb["otdr_max"], "dB", "vhl")
        line(22, "ค่า attenuation โดยรวมของเส้นทางเมื่อทดสอบด้วยเครื่อง OTDR (Min.)", lb["otdr_min"], "dB", "vhl")
        ws.write(24, 1, "2) กรณี Wavelength เป็น 1550 nm. ค่า Attenuation (aF) = 0.20dB/Km.", F["txt"])
        ws.write(25, 1, "3) ผล Test OTDR ต้อง Test มาไม่เกิน 1 เดือนก่อนส่งงาน", F["txt"])
        ws.write(27, 1, "หมายเหตุ :", self.wb.add_format({"font_name": FONT, "font_size": 14, "bold": True, "underline": True}))
        ws.merge_range(28, 1, 28, 5, f.get("lb_note") or f"Wavelength {f.get('lb_wavelength') or '1550'} nm", F["u"])
        for r in (29, 30, 31):
            ws.merge_range(r, 1, r, 5, "", F["u"])
        ws.print_area(0, 0, 32, 6)

    # ---------------------------------------------------------- หน้าที่วางเป็นภาพ
    def image_pages(self, title, imgs):
        for i, (b, w_pt, h_pt, sc) in enumerate(imgs, 1):
            ws = self.sheet(title if len(imgs) == 1 else f"{title} {i}", landscape=w_pt > h_pt)
            ws.set_margins(0.2, 0.2, 0.2, 0.2)
            ws.insert_image(0, 0, "pg.jpg", {"image_data": b, "x_scale": sc, "y_scale": sc, "object_position": 3})
            ws.print_area(0, 0, int(h_pt / 15) + 1, int(w_pt / COLPT) + 1)

    def html_page(self, title, html):
        self.image_pages(title, _pdf_images(pdfgen.render_html([html])))

    def close(self):
        self.wb.close()
        return self.buf.getvalue()


def build_xlsx(job, company, pages, photos_by_slot, logo_l, logo_r, sheet_pdf=None, sheet_cfg=None, sors=None, asbuilt_pdfs=None):
    """asbuilt_pdfs=None -> เล่ม PAT ; เป็น list -> เล่ม AS-BUILT (แทรกแบบ As-Built ก่อน Report OTDR)"""
    bk = Book()
    sheet_cfg = sheet_cfg or {}
    for page in pages:
        k = page["kind"]
        if k == "cover":
            bk.html_page("ปก", pdfgen.page_cover(job, company, logo_l, logo_r))
        elif k == "checklist":
            bk.html_page("Checklist", pdfgen.page_checklist(job, logo_l, logo_r))
        elif k == "inspection":
            bk.html_page("สรุปผลตรวจรับ", pdfgen.page_inspection(job, company, logo_l, logo_r))
        elif k == "sheet":
            if sheet_pdf:
                n = pdfgen.count_pages(sheet_pdf)
                bk.image_pages("Sheet", _pdf_images(sheet_pdf, pdfgen.parse_ranges(sheet_cfg.get("forms", ""), n)))
        elif k == "photos":
            bk.photos(page, photos_by_slot, logo_l, logo_r)
        elif k == "single":
            bk.single(page, photos_by_slot, logo_l, logo_r)
        elif k == "linkbudget":
            bk.linkbudget(job)
        elif k == "deficiency":
            bk.html_page("Deficiency", pdfgen.page_deficiency(job, company, logo_l))
    for b in (asbuilt_pdfs or []):
        bk.image_pages("As-Built", _pdf_images(b))
    if sors:
        bk.image_pages("Report OTDR", _pdf_images(pdfgen.render_html([pdfgen.page_otdr(s) for s in sors])))
    return bk.close(), bk.n
