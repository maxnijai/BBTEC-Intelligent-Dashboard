"""
สร้าง PDF: หน้าที่ระบบสร้างเอง (ปก, หน้ารูป, Link Budget, Deficiency) ด้วย WeasyPrint
แล้วรวมกับหน้าฟอร์มจาก Google Sheet และไฟล์ AS-BUILT (แบบ/OTDR) ด้วย pypdf
"""
import html
import io
import os
import re

from pypdf import PdfReader, PdfWriter
from weasyprint import HTML

from .layout import link_budget, sites_from_link

FONT_DIR = os.path.join(os.path.dirname(__file__), "static", "fonts")


def _uri(path):
    return "file://" + os.path.abspath(path)


BASE_CSS = f"""
@font-face {{ font-family: SarabunL; src: url('{_uri(os.path.join(FONT_DIR, 'Sarabun-latin-Regular.ttf'))}'); font-weight: 400; }}
@font-face {{ font-family: SarabunL; src: url('{_uri(os.path.join(FONT_DIR, 'Sarabun-latin-Bold.ttf'))}'); font-weight: 700; }}
@font-face {{ font-family: SarabunT; src: url('{_uri(os.path.join(FONT_DIR, 'Sarabun-thai-Regular.ttf'))}'); font-weight: 400; }}
@font-face {{ font-family: SarabunT; src: url('{_uri(os.path.join(FONT_DIR, 'Sarabun-thai-Bold.ttf'))}'); font-weight: 700; }}
@page {{ size: Letter; margin: 0.55in 0.6in; }}
* {{ box-sizing: border-box; }}
body {{ font-family: SarabunL, SarabunT, sans-serif; font-size: 10.5pt; color: #000; margin: 0; }}
.page {{ page-break-after: always; height: 9.85in; }}
.page:last-child {{ page-break-after: auto; }}
/* หน้ารูป 3 ช่อง */
table.ph {{ width: 100%; height: 100%; border-collapse: collapse; border: 1.6pt solid #000; table-layout: fixed; }}
table.ph td {{ border: 1.6pt double #000; text-align: center; vertical-align: middle; padding: 4pt 6pt; }}
td.hd {{ height: 0.48in; padding: 0 8pt !important; }}
.hd-row {{ display: flex; align-items: center; justify-content: space-between; }}
.hd-row img.l {{ height: 0.34in; }}
.hd-row img.r {{ height: 0.36in; }}
.hd-row .t {{ flex: 1; text-align: center; font-weight: 700; font-size: 11pt; }}
td.cell {{ height: 3.1in; }}
td.cell img {{ max-width: 4.55in; max-height: 2.72in; display: block; margin: 0 auto 4pt; }}
.cap {{ font-weight: 700; text-decoration: underline; font-size: 9.5pt; }}
.na {{ font-size: 22pt; font-weight: 700; }}
td.big {{ height: 9.2in; }}
td.big img {{ max-width: 6.8in; max-height: 8.9in; display: block; margin: 0 auto; }}
/* ปก */
.cover {{ text-align: center; padding-top: 1.0in; }}
.cover img.l {{ height: 0.62in; }}
.cover img.r {{ height: 0.5in; display: block; margin: 2pt auto 0; }}
.cover .h1 {{ font-size: 22pt; margin: 4pt 0 10pt; }}
.cover .h2 {{ font-size: 16pt; margin: 0 0 8pt; }}
.cover .meta {{ font-size: 13pt; line-height: 1.75; margin-top: 26pt; }}
.cover .ln {{ font-size: 16pt; margin-top: 30pt; }}
.cover .lv {{ font-weight: 700; font-size: 12.5pt; margin-top: 10pt; }}
.cover .co {{ font-weight: 700; font-size: 17pt; margin-top: 70pt; }}
.cover .addr {{ font-size: 9.5pt; line-height: 1.9; white-space: pre-line; }}
/* link budget */
.lb {{ border: 1.4pt solid #000; height: 100%; }}
.lb .top {{ background: #00FFFF; border-bottom: 1.2pt solid #000; padding: 10pt 12pt; display: flex; align-items: center; }}
.lb .top .t {{ flex: 1; text-align: center; font-weight: 700; font-size: 11pt; }}
.lb .top img {{ height: 0.3in; }}
.lb .ver {{ text-align: right; }}
.lb .ver span {{ background: #00B0F0; font-weight: 700; font-size: 8.5pt; padding: 3pt 10pt; display: inline-block; }}
.lb table {{ border-collapse: collapse; margin: 0 auto; font-size: 9.5pt; }}
.lb td {{ padding: 4pt 6pt; }}
.lb .v {{ border-bottom: 0.8pt solid #000; text-align: center; min-width: 0.7in; }}
.lb .hl {{ background: #9FFFFF; }}
.lb .yl {{ background: #FFFF00; }}
.lb .formula {{ border: 1pt solid #000; padding: 5pt 30pt; text-align: center; margin: 14pt auto; width: 5.6in; }}
.lb .note {{ margin: 14pt 30pt; font-size: 9pt; }}
.lb .lines div {{ border-bottom: 0.8pt solid #000; height: 18pt; margin: 0 30pt; }}
/* deficiency */
.df h3 {{ text-align: center; margin: 4pt 0; font-size: 12pt; }}
.df .bar {{ height: 8pt; background: #FFFF00; margin-bottom: 6pt; }}
.df table {{ width: 100%; border-collapse: collapse; font-size: 8.5pt; }}
.df td, .df th {{ border: 0.8pt solid #000; height: 15pt; padding: 1pt 4pt; }}
.df .sig td {{ background: #F2DCDB; text-align: center; font-weight: 700; }}
.df .sig td.w {{ background: #fff; }}
"""


def esc(s):
    return html.escape(str(s if s is not None else ""))


def _hdr(title, logo_l, logo_r):
    l = f'<img class="l" src="{_uri(logo_l)}">' if logo_l else "<span></span>"
    r = f'<img class="r" src="{_uri(logo_r)}">' if logo_r else "<span></span>"
    return f'<td class="hd"><div class="hd-row">{l}<div class="t">{esc(title)}</div>{r}</div></td>'


def page_photos(page, photos_by_slot, logo_l, logo_r):
    rows = [f"<tr>{_hdr(page['title'], logo_l, logo_r)}</tr>"]
    for s in page["slots"]:
        p = photos_by_slot.get(s["key"])
        if p == "NA" or (p is None and not s["caption"]):
            inner = '<div class="na">N/A</div>'
        elif p is None:
            inner = f'<div class="na" style="color:#C00">ไม่มีรูป</div><div class="cap">{esc(s["caption"])}</div>'
        else:
            inner = f'<img src="{_uri(p)}"><div class="cap">{esc(s["caption"])}</div>'
        rows.append(f'<tr><td class="cell">{inner}</td></tr>')
    return f'<div class="page"><table class="ph">{"".join(rows)}</table></div>'


def page_single(page, photos_by_slot, logo_l, logo_r):
    p = photos_by_slot.get(page["slots"][0]["key"])
    inner = f'<img src="{_uri(p)}">' if p and p != "NA" else '<div class="na">N/A</div>'
    return (f'<div class="page"><table class="ph"><tr>{_hdr(page["title"], logo_l, logo_r)}</tr>'
            f'<tr><td class="big">{inner}</td></tr></table></div>')


def page_cover(job, company, logo_l, logo_r):
    f = job["fields"]
    l = f'<img class="l" src="{_uri(logo_l)}">' if logo_l else ""
    r = f'<img class="r" src="{_uri(logo_r)}">' if logo_r else ""
    return f"""<div class="page"><div class="cover">{l}{r}
      <div class="h1">Provisional Acceptance Test (PAT)</div>
      <div class="h2">Optical Fiber Cable (OFC)</div>
      <div>Version: {esc(job.get('template_version', '3.0'))}</div>
      <div class="meta">SAP No. : {esc(f.get('sap_no'))}<br>Project : {esc(f.get('project'))}<br>
        Region : {esc(f.get('region'))}<br>Activity : {esc(f.get('activity'))}</div>
      <div class="ln">Link Name</div><div class="lv">{esc(job['link_name'])}</div>
      <div class="co">{esc(company.get('name_th') or company.get('name'))}</div>
      <div class="addr">{esc(company.get('address'))}</div></div></div>"""


def _n(v, d=2):
    return "" if v is None else f"{v:.{d}f}"


def page_linkbudget(job, logo_l):
    f = job["fields"]
    s1, s2 = sites_from_link(job["link_name"])
    lb = link_budget(f)
    img = f'<img src="{_uri(logo_l)}">' if logo_l else ""
    rows = [
        ("L = ความยาวของ Optical Fiber", _n(lb["L"], 3), "Km.", "hl", "ระยะจาก OTDR"),
        ("aF = ค่า Attenuation ของ fiber", _n(lb["aF"]), "dB/Km.", "", ""),
        ("NE = จำนวนของจุด splice ไม่รวมใน ODF", _n(lb["NE"]), "Point", "hl", ""),
        ("aE = ค่าเฉลี่ยของ attenuation ของจุด Splice รวมกันทุกจุด", _n(lb["aE"]), "dB", "", ""),
        ("NC = จำนวนของ Connector", _n(lb["NC"]), "Point", "hl", ""),
        ("aC = ค่าเฉลี่ยของ attenuation ต่อ connector รวม Splice ใน ODF", _n(lb["aC"]), "dB", "", ""),
    ]
    body = "".join(f'<tr><td>{a}</td><td>=</td><td class="v {c}">{b}</td><td>{u}</td><td style="color:#C00;font-weight:700;font-size:8pt">{x}</td></tr>' for a, b, u, c, x in rows)
    calc = [("ค่า attenuation โดยรวมของ Cable ทั้งเส้นทาง (LxaF)", lb["cable"], ""),
            ("ค่า attenuation โดยรวมของจุด Splice ทั้งเส้นทาง (NExaE)", lb["splice"], ""),
            ("ค่า attenuation โดยรวมของ Connector ทั้งเส้นทาง (NCxaC)", lb["conn"], "")]
    body2 = "".join(f'<tr><td>{a}</td><td>=</td><td class="v">{_n(b)}</td><td>dB</td></tr>' for a, b, _ in calc)
    tot = [("ค่า attenuation โดยรวมของเส้นทางโดยประมาณ (จากการคำนวณ)", lb["total"], "yl"),
           ("ค่า attenuation โดยรวมของเส้นทางเมื่อทดสอบด้วยเครื่อง OTDR (Max.)", lb["otdr_max"], "hl"),
           ("ค่า attenuation โดยรวมของเส้นทางเมื่อทดสอบด้วยเครื่อง OTDR (Min.)", lb["otdr_min"], "hl")]
    body3 = "".join(f'<tr><td>{a}</td><td>=</td><td class="v {c}">{_n(b)}</td><td>dB</td></tr>' for a, b, c in tot)
    wl = esc(f.get("lb_wavelength") or "1550")
    return f"""<div class="page"><div class="lb">
      <div class="top"><div class="t">8.2 OPTICAL FIBER LINK BUDGET</div>{img}</div>
      <div class="ver"><span>PAT Doc. Version: 6.0</span></div>
      <table style="margin-top:14pt"><tr><td style="text-align:right">Link : Name of station A :</td><td class="v" style="text-align:left;min-width:3in">{esc(s2)}</td></tr>
      <tr><td style="text-align:right">Link : Name of station B :</td><td class="v" style="text-align:left">{esc(s1)}</td></tr>
      <tr><td style="text-align:right">Route Name :</td><td class="v" style="text-align:left">{esc(s2)} - {esc(s1)}</td></tr></table>
      <div class="formula">ค่า Attenuation โดยรวม (dB) = ( L x aF ) + ( NE X aE ) + ( NC X aC )</div>
      <table>{body}</table><br><table>{body2}</table><br><table>{body3}</table>
      <div class="note">2) กรณี Wavelength เป็น 1550 nm. ค่า Attenuation (aF) = 0.20dB/Km.<br>3) ผล Test OTDR ต้อง Test มาไม่เกิน 1 เดือนก่อนส่งงาน</div>
      <div style="margin:0 30pt;font-weight:700;text-decoration:underline">หมายเหตุ :</div>
      <div class="lines"><div style="text-align:center">{esc(f.get('lb_note') or ('Wavelength ' + wl + ' nm'))}</div><div></div><div></div><div></div></div>
    </div></div>"""


def page_deficiency(job, company, logo_l):
    rows = "".join("<tr><td></td><td></td><td></td><td></td></tr>" for _ in range(26))
    img = f'<img src="{_uri(logo_l)}" style="height:0.22in;float:right">' if logo_l else ""
    return f"""<div class="page df"><div class="bar"></div>{img}
      <h3>DEFFICIENCY LIST FOR PHYSICAL (ODN)</h3><h3>{esc(job['fields'].get('region'))}</h3><h3>{esc(job['link_name'])}</h3>
      <h3>Date ............./............./.............</h3><h3>Sub-Contractor name : {esc(company.get('name'))}</h3>
      <table><tr><th style="width:9%">Item</th><th style="width:12%">Category</th><th>Description</th><th style="width:24%">Clear / Acknowledgement</th></tr>{rows}</table>
      <table class="sig" style="margin-top:8pt"><tr><td>สำหรับ</td><td>Contractor</td><td colspan="2" style="color:#C00">True Inspector Approve</td></tr>
      <tr><td>Department</td><td>{esc(company.get('name'))}</td><td>Online Implementation</td><td>Network Operation (NOP)</td></tr>
      <tr><td>ลายเซ็น</td><td class="w"></td><td class="w"></td><td class="w"></td></tr>
      <tr><td>ชื่อตัวบรรจง</td><td class="w"></td><td class="w"></td><td class="w"></td></tr>
      <tr><td>วันที่</td><td class="w"></td><td class="w"></td><td class="w"></td></tr></table></div>"""


def render_html(pages_html):
    doc = f"<html><head><meta charset='utf-8'><style>{BASE_CSS}</style></head><body>{''.join(pages_html)}</body></html>"
    return HTML(string=doc, base_url="/").write_pdf()

# ---------------------------------------------------------------- sheet PDF helpers


def parse_ranges(spec, n):
    out = []
    for part in re.split(r"[,\s]+", (spec or "").strip()):
        if not part:
            continue
        if part.lower() == "last":
            out.append(n - 1)
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            a = int(a) if a else 1
            b = n if b.lower() in ("", "last") else int(b)
            out.extend(range(a - 1, min(b, n)))
        else:
            i = int(part) - 1
            if 0 <= i < n:
                out.append(i)
    return [i for i in out if 0 <= i < n]


KEYS_FORM = ("รายการเอกสาร", "CHECK LIST", "COMMENT", "LOSS", "BALANCE", "MATERIAL", "ใบสรุป", "สรุปผลการตรวจ", "ASSEMBLY")
KEYS_SKIP = ("PROVISIONAL ACCEPTANCE TEST",)


def detect_sheet_pages(pdf_bytes):
    """เดาว่าหน้าไหนในไฟล์จาก Google Sheet เป็นหน้าฟอร์ม / Link budget / Deficiency"""
    r = PdfReader(io.BytesIO(pdf_bytes))
    forms, lb, dfc = [], None, None
    for i, pg in enumerate(r.pages):
        try:
            t = (pg.extract_text() or "").upper()
        except Exception:
            t = ""
        compact = re.sub(r"\s+", "", t)
        if "DEFFICIENCY" in compact or "DEFICIENCY" in compact:
            dfc = i
        elif "LINKBUDGET" in compact:
            lb = i
        elif any(k.replace(" ", "") in compact for k in KEYS_SKIP) and i == 0:
            continue
        elif any(k.replace(" ", "") in compact for k in KEYS_FORM):
            forms.append(i)
    fmt = lambda xs: ",".join(str(x + 1) for x in xs)
    return {"forms": fmt(forms), "linkbudget": "" if lb is None else str(lb + 1),
            "deficiency": "" if dfc is None else str(dfc + 1), "count": len(r.pages)}

# ---------------------------------------------------------------- build


def build_pat(job, company, pages, photos_by_slot, logo_l, logo_r, sheet_pdf=None, sheet_cfg=None):
    """คืน bytes ของ PAT.pdf"""
    sheet_cfg = sheet_cfg or {}
    reader = PdfReader(io.BytesIO(sheet_pdf)) if sheet_pdf else None
    n = len(reader.pages) if reader else 0
    use_sheet_lb = reader is not None and sheet_cfg.get("linkbudget") and not job["fields"].get("lb_L")
    writer = PdfWriter()
    buf = []

    def flush():
        if buf:
            pdf = render_html(buf)
            for p in PdfReader(io.BytesIO(pdf)).pages:
                writer.add_page(p)
            buf.clear()

    def add_sheet(spec):
        flush()
        for i in parse_ranges(spec, n):
            writer.add_page(reader.pages[i])

    for page in pages:
        k = page["kind"]
        if k == "cover":
            buf.append(page_cover(job, company, logo_l, logo_r))
        elif k == "sheet":
            if reader:
                add_sheet(sheet_cfg.get("forms", ""))
        elif k == "photos":
            buf.append(page_photos(page, photos_by_slot, logo_l, logo_r))
        elif k == "single":
            buf.append(page_single(page, photos_by_slot, logo_l, logo_r))
        elif k == "linkbudget":
            if use_sheet_lb:
                add_sheet(sheet_cfg["linkbudget"])
            else:
                buf.append(page_linkbudget(job, logo_l))
        elif k == "deficiency":
            if reader and sheet_cfg.get("deficiency"):
                add_sheet(sheet_cfg["deficiency"])
            else:
                buf.append(page_deficiency(job, company, logo_l))
    flush()
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def build_asbuilt(pat_bytes, extra_pdfs):
    """AS-BUILT = PAT + แบบ As-Built (ปก, Symbol, Core Assignment) + Report OTDR"""
    w = PdfWriter()
    for p in PdfReader(io.BytesIO(pat_bytes)).pages:
        w.add_page(p)
    for b in extra_pdfs:
        for p in PdfReader(io.BytesIO(b)).pages:
            w.add_page(p)
    out = io.BytesIO()
    w.write(out)
    return out.getvalue()


def count_pages(pdf_bytes):
    return len(PdfReader(io.BytesIO(pdf_bytes)).pages)
