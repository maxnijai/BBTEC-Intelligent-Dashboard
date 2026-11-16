"""
สร้าง PDF ด้วย WeasyPrint (ขนาด/ตำแหน่งตาม Template PAT OFC v3.0 วัดจากไฟล์ตัวอย่าง หน่วย pt)
แล้วรวมกับหน้าจาก Google Sheet และไฟล์ AS-BUILT ด้วย pypdf
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


def esc(s):
    return html.escape(str(s if s is not None else ""))


BASE_CSS = f"""
@font-face {{ font-family: SarabunL; src: url('{_uri(os.path.join(FONT_DIR, 'Sarabun-latin-Regular.ttf'))}'); font-weight: 400; }}
@font-face {{ font-family: SarabunL; src: url('{_uri(os.path.join(FONT_DIR, 'Sarabun-latin-Bold.ttf'))}'); font-weight: 700; }}
@font-face {{ font-family: SarabunT; src: url('{_uri(os.path.join(FONT_DIR, 'Sarabun-thai-Regular.ttf'))}'); font-weight: 400; }}
@font-face {{ font-family: SarabunT; src: url('{_uri(os.path.join(FONT_DIR, 'Sarabun-thai-Bold.ttf'))}'); font-weight: 700; }}
@page {{ size: 612pt 792pt; margin: 0; }}
@page land {{ size: 792pt 612pt; margin: 0; }}
* {{ box-sizing: border-box; }}
body {{ font-family: SarabunL, SarabunT, sans-serif; color: #000; margin: 0; font-size: 8pt; }}
.pg {{ position: relative; width: 612pt; height: 792pt; page-break-after: always; overflow: hidden; }}
.pg.land {{ page: land; width: 792pt; height: 612pt; }}
.pg:last-child {{ page-break-after: auto; }}
.abs {{ position: absolute; }}
/* ---- หน้ารูป */
.frame {{ position: absolute; border: 1.2pt solid #000; }}
.hdr {{ position: absolute; left: 0; right: 0; top: 0; border-bottom: 2.4pt double #000; }}
.hdr .t {{ position: absolute; left: 0; right: 0; text-align: center; font-weight: 700; }}
.row {{ position: absolute; left: 0; right: 0; border-bottom: 2.4pt double #000; }}
.row:last-child {{ border-bottom: none; }}
.row img.ph {{ position: absolute; object-fit: fill; }}
.row .cap {{ position: absolute; left: 0; right: 0; text-align: center; font-weight: 700; text-decoration: underline; font-size: 9pt; }}
.na {{ position: absolute; left: 0; right: 0; text-align: center; font-weight: 700; font-size: 20pt; }}
.miss {{ color: #C00; }}
/* ---- ตาราง */
table.g {{ border-collapse: collapse; table-layout: fixed; width: 100%; }}
table.g td, table.g th {{ border: 0.6pt solid #000; padding: 0 3pt; height: 12pt; font-weight: 400; overflow: hidden; white-space: nowrap; }}
.c {{ text-align: center; }} .r {{ text-align: right; }} .b {{ font-weight: 700; }}
.cy {{ background: #66FFFF; }} .pk {{ background: #E6B8B7; }} .gy {{ background: #BFBFBF; }}
.sig img {{ max-height: 22pt; max-width: 110pt; }}
.vt {{ position: absolute; left: 0; top: 24pt; width: 50pt; height: 19pt; transform-origin: 0 0; transform: rotate(-90deg);
      font-size: 5.6pt; white-space: nowrap; text-align: center; line-height: 19pt; }}
"""


def _logo(path, cls, style):
    return f'<img class="abs {cls}" src="{_uri(path)}" style="{style}">' if path else ""

# ================================================================ หน้ารูป


GEOM = {  # x, y, w, header_h, row_h, img_dx, img_dy, img_w, img_h, cap_dy, title_pt
    "photos": (129, 77, 400, 28, 203, 57, 7, 273.4, 175.4, 186, 10),
    "closure": (131, 63, 410, 28, 212.3, 59, 8, 280, 180, 196, 10),
}


def page_photos(page, photos_by_slot, logo_l, logo_r):
    g = GEOM["closure" if page["slots"] and page["slots"][0]["key"].startswith("closure:") else "photos"]
    x, y, w, hh, rh, dx, dy, iw, ih, cdy, tpt = g
    h = hh + rh * 3
    out = [f'<div class="frame" style="left:{x}pt;top:{y}pt;width:{w}pt;height:{h}pt">',
           f'<div class="hdr" style="height:{hh}pt">'
           f'{_logo(logo_l, "", f"left:2pt;top:1pt;height:{hh - 2}pt")}'
           f'<div class="t" style="top:{(hh - tpt * 1.5) / 2}pt;font-size:{tpt}pt">{esc(page["title"])}</div>'
           f'{_logo(logo_r, "", f"right:4pt;top:2pt;height:{hh - 5}pt")}</div>']
    for i, s in enumerate(page["slots"]):
        top = hh + i * rh
        p = photos_by_slot.get(s["key"])
        out.append(f'<div class="row" style="top:{top}pt;height:{rh}pt">')
        if p == "NA" or (p is None and not s["caption"]):
            out.append(f'<div class="na" style="top:{rh / 2 - 14}pt">N/A</div>')
        else:
            if p:
                out.append(f'<img class="ph" src="{_uri(p)}" style="left:{dx}pt;top:{dy}pt;width:{iw}pt;height:{ih}pt">')
            else:
                out.append(f'<div class="na miss" style="top:{rh / 2 - 20}pt;font-size:13pt">ไม่มีรูป</div>')
            out.append(f'<div class="cap" style="top:{cdy}pt">{esc(s["caption"])}</div>')
        out.append("</div>")
    out.append("</div>")
    return f'<div class="pg">{"".join(out)}</div>'


def page_single(page, photos_by_slot, logo_l, logo_r):
    """Topo Design / Support Planwork : กระดาษแนวนอนตาม Template"""
    x, y, w, h, hh = 77, 110, 669, 422, 27
    p = photos_by_slot.get(page["slots"][0]["key"])
    img = (f'<img src="{_uri(p)}" style="max-width:{w - 50}pt;max-height:{h - hh - 40}pt;display:block;margin:auto">'
           if p and p != "NA" else '<div style="font-size:20pt;font-weight:700">N/A</div>')
    return (f'<div class="pg land"><div class="frame" style="left:{x}pt;top:{y}pt;width:{w}pt;height:{h}pt">'
            f'<div class="hdr" style="height:{hh}pt">{_logo(logo_l, "", f"left:2pt;top:1pt;height:{hh - 2}pt")}'
            f'<div class="t" style="top:5pt;font-size:11pt">{esc(page["title"])}</div>'
            f'{_logo(logo_r, "", f"right:4pt;top:2pt;height:{hh - 4}pt")}</div>'
            f'<div style="position:absolute;left:0;right:0;top:{hh}pt;bottom:0;display:flex;align-items:center;justify-content:center">{img}</div>'
            f'</div></div>')

# ================================================================ ปก


def page_cover(job, company, logo_l, logo_r):
    f = job["fields"]
    return f"""<div class="pg"><div style="position:absolute;left:0;right:0;top:80pt;text-align:center">
      {f'<img src="{_uri(logo_l)}" style="height:44pt">' if logo_l else ''}
      {f'<div><img src="{_uri(logo_r)}" style="height:36pt;margin-top:2pt"></div>' if logo_r else ''}
      <div style="font-size:22pt;margin:6pt 0 10pt">Provisional Acceptance Test (PAT)</div>
      <div style="font-size:16pt;margin-bottom:8pt">Optical Fiber Cable (OFC)</div>
      <div style="font-size:10pt">Version: {esc(job.get('template_version', '3.0'))}</div>
      <div style="font-size:13pt;line-height:1.75;margin-top:24pt">SAP No. : {esc(f.get('sap_no'))}<br>Project : {esc(f.get('project'))}<br>
        Region : {esc(f.get('region'))}<br>Activity : {esc(f.get('activity'))}</div>
      <div style="font-size:16pt;margin-top:28pt">Link Name</div>
      <div style="font-size:12.5pt;font-weight:700;margin-top:10pt">{esc(job['link_name'])}</div>
      <div style="font-size:17pt;font-weight:700;margin-top:70pt">{esc(company.get('name_th') or company.get('name'))}</div>
      <div style="font-size:9.5pt;line-height:1.9;white-space:pre-line">{esc(company.get('address'))}</div></div></div>"""

# ================================================================ ลายเซ็น


def _sig(f, role):
    img = f.get(f"sig_{role}_img")
    return {"name": f.get(f"sig_{role}_name", ""), "date": f.get(f"sig_{role}_date", ""), "pos": f.get(f"sig_{role}_pos", ""),
            "img": f'<img src="{_uri(img)}">' if img and os.path.exists(img) else ""}

# ================================================================ รายการเอกสารส่งงาน (Check list)


CHECK_ROWS = [  # (ข้อความ, จำนวน, ขีด, เทา, กลุ่ม)
    ("1. รายการเอกสารส่งงานติดตั้ง(Check listใบปะหน้า)", "1", "/", 0, "acc"),
    ("2. ใบ Final BOQ. (กระดาษ A4)", "1", "/", 0, "acc"),
    ("3. ใบ เอกสารสรุป Assembly Units &amp; Material Unit  ไม่มีราคา (กระดาษ A4)", "1", "", 0, "acc"),
    ("4. แบบเส้นทางโดยรวมทั้งโครงการ (Key - Map)) (กระดาษ A4)", "1", "/", 0, "acc"),
    ("1. สรุปผลการตรวจรับ (Check list &amp; Comment Sheet) (กระดาษ A4) ตัวจริง", "1", "/", 0, "tm"),
    ("2. แบบเส้นทางโดยรวมทั้งโครงการ (Key - Map)) (กระดาษ A4)", "1", "", 0, "tm"),
    ("3. ใบ Outside Plant Job Requirement (OR)  (กระดาษ A4)", "1", "", 0, "tm"),
    ("4. เอกสารสรุป Assembly Units &amp; Material Unit  ไม่มีราคา (กระดาษ A4)", "1", "", 0, "tm"),
    ("5. เอกสารผลการทดสอบระบบ Ground (Ground Test) (กระดาษ A4)", "1", "", 0, "tm"),
    ("6. สรุปการใช้เคเบิล และวัสดุอุปกรณ์ของ BBTEC (BalanceCable &amp; Material) (กระดาษ A4)", "1", "", 0, "tm"),
    ("7. รูปถ่าย(สี) ก่อน และ/หรือ หลังดำเนินการ (2รูป:กระดาษ A4) โดยระบุวัน เดือน ปี/Location จุดที่ถ่ายภาพ โดยดูได้ชัดเจน", "1", "/", 0, "tm"),
    ("8. เอกสารผลการตรวจสอบ Core Fiber (OTDR Test)", "", "", 1, "tm"),
    ("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;8.1 ตารางบันทึกการทดสอบการวัดค่า Loss OTDR ของ Optical Fiber (กระดาษ A4)", "1", "/", 0, "tm"),
    ("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;8.2 ผล OTDR Test ที่แสดง Trace ,Event table และ Parameter ทั้งหมด (A4 และ CD)", "1", "/", 0, "tm"),
    ("9. เอกสารแบบ MEA &amp; PEA Permission (ระบุวันที่ส่งแบบ)", "1", "", 0, "tm"),
    ("10.  เอกสารขออนุมัติเพิ่มจุดต่อ (ปริ้นแนบ)", "1", "", 0, "tm"),
    ("1. เอกสารส่งงานชุด As-Built Drawing (กระดาษ A4)", "", "", 1, "tm"),
    ("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.1 ใบปะหน้าเอกสาร (Cover Sheet) และเอกสารสัญลักษณ์ที่ใช้ในการเขียนแบบ (Symbol)", "1", "/", 0, "tm"),
    ("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.2 เอกสารเส้นทางโดยรวมทั้งโครงการ (Key - Map)   (กระดาษ A4)", "1", "/", 0, "tm"),
    ("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.3 เอกสารแสดงเส้นทางโดยละเอียด (Detail Design)  Scale 1:1000", "1", "/", 0, "tm"),
    ("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.4 เอกสารแจ้งจุดตัดต่อในแต่ละเส้นทาง (Joint Map) (กระดาษ A4)", "1", "", 0, "tm"),
    ("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.5 เอกสารแสดงรายละเอียดจุดตัดต่อ(Core Management) (กระดาษA4 )", "1", "/", 0, "tm"),
    ("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.6 แบบแสดงตำแหน่ง ODF", "1", "/", 0, "tm"),
    ("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.7 แบบติดตั้งท่อและเดินสายกับอาคาร", "1", "", 0, "tm"),
    ("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.8 CD Soft File ข้อมูลตาม 1.1 - 1.7", "1", "", 0, "tm"),
    ("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.9 ลงข้อมูล GIS ตาม 1.2 - 1.7", "1", "", 0, "tm"),
]


def page_checklist(job, logo_l, logo_r):
    f = job["fields"]
    snd = _sig(f, "sender")
    rows = []
    acc_n = sum(1 for r in CHECK_ROWS if r[4] == "acc")
    tm_n = len(CHECK_ROWS) - acc_n
    first = {"acc": True, "tm": True}
    for text, qty, tick, grey, grp in CHECK_ROWS:
        part = ""
        if first[grp]:
            first[grp] = False
            if grp == "acc":
                part = f'<td rowspan="{acc_n}" class="c b" style="font-size:12pt">บัญชี</td>'
            else:
                part = f'<td rowspan="{tm_n}" class="c" style="font-size:9pt;white-space:normal">Transport Media</td>'
        q = "c gy" if grey else "c"
        rows.append(f'<tr><td style="font-size:6.3pt">{text}</td><td class="{q}">{qty}</td><td class="c">{tick}</td><td></td>{part}</tr>')
    lines = "".join('<tr><td style="height:12.3pt"></td><td></td></tr>' for _ in range(6))
    return f"""<div class="pg"><div class="abs" style="left:44pt;top:18pt;width:553pt">
<table class="g">
 <colgroup><col style="width:315pt"><col style="width:20pt"><col style="width:20pt"><col style="width:123pt"><col style="width:75pt"></colgroup>
 <tr><td colspan="4" style="height:26pt"><div style="display:flex;align-items:center;height:24pt">
   <div style="width:100pt;padding-left:18pt">{f'<img src="{_uri(logo_l)}" style="height:18pt">' if logo_l else ''}</div>
   <div style="flex:1;font-size:11pt;color:#0000FF;font-weight:700">รายการเอกสารส่งงานติดตั้ง Optical Fiber Cable</div>
   <div style="width:60pt">{f'<img src="{_uri(logo_r)}" style="height:18pt">' if logo_r else ''}</div></div></td>
   <td style="font-size:6.3pt;vertical-align:top">OR No. {esc(f.get('or_no'))}</td></tr>
 <tr><td style="height:26pt"><span style="font-size:10pt">เส้นทาง :</span><span style="position:relative;left:60pt;font-size:9pt">{esc(job['link_name'])}</span></td>
   <td colspan="3" style="font-size:6.3pt">OFC &nbsp; ({esc(f.get('ofc'))}) &nbsp; Core {esc(f.get('core'))}</td><td style="font-size:6.3pt">SAP Job No. : {esc(f.get('sap_no'))}</td></tr>
 <tr><td class="c b" style="height:56pt;font-size:10.5pt">รายการเอกสารส่งงาน ตามมาตรฐาน BBTEC</td>
   <td style="padding:0;height:56pt;overflow:hidden;position:relative"><div class="vt">จำนวนเอกสาร (ชุด)</div></td><td style="padding:0;overflow:hidden;position:relative"><div class="vt">ขีด /ช่องเอกสารที่ส่ง</div></td>
   <td class="c b" style="font-size:10.5pt">หมายเหตุ</td><td class="c b" style="font-size:10.5pt">เอกสารส่วนที่</td></tr>
 {''.join(rows)}
 <tr><td colspan="5" style="font-size:6.3pt;color:#C00;font-weight:700">เอกสาร Job จัดสรร Core &nbsp; 1.บัญชี (ข้อ 1, 2, 3) &nbsp;2.Outside Plant (ข้อ 1, 3, 4, 6, 7, 8, Joint Map, Core Management, CD Soft File)</td></tr>
 <tr><td colspan="5" style="font-size:10pt;color:#C00;font-weight:700;height:18pt;text-decoration:underline">หมายเหตุ เอกสารทุกส่วนต้องเรียงตามรายการและมีเลขที่ SAP Job และ SAP PO ด้านหน้าแต่ละส่วน</td></tr>
</table>
<table class="g"><colgroup><col style="width:315pt"><col style="width:238pt"></colgroup>
 <tr><td class="b" style="text-decoration:underline;font-size:6.5pt">หมายเหตุ (สำหรับ --Name Contractor--)  ครั้งที่ 1</td><td class="b" style="text-decoration:underline;font-size:6.5pt">หมายเหตุ (สำหรับ--Name Contractor--)  ครั้งที่ 2</td></tr>
 {lines}
 <tr><td style="font-size:6.3pt;font-style:italic">ส่งเอกสารวันที่ &nbsp; {esc(snd['date'])}</td><td style="font-size:6.3pt;font-style:italic">ส่งเอกสารวันที่ ______________________</td></tr>
 <tr class="sig"><td style="font-size:6.3pt;height:18pt;font-weight:700;font-style:italic">ผู้ส่งเอกสาร <span style="display:inline-block;width:110pt;border-bottom:0.6pt solid #000;text-align:center;font-style:normal">{snd['img'] or esc(snd['name'])}</span> ตำแหน่ง <span style="display:inline-block;width:100pt;border-bottom:0.6pt solid #000;text-align:center;font-style:normal">{esc(snd['pos'])}</span></td>
   <td style="font-size:6.3pt;font-weight:700;font-style:italic">ผู้ส่งเอกสาร __________________________ ตำแหน่ง ________________________</td></tr>
 <tr><td style="height:12pt"></td><td></td></tr>
 <tr><td colspan="2" class="b" style="text-decoration:underline;font-size:6.5pt;height:14pt">ผลการตรวจ (สำหรับ OSP &amp; Coordination Division --Name Contractor--)</td></tr>
 <tr><td style="font-size:6.3pt"><b><u>ตรวจครั้งที่ 1</u></b> <span style="margin-left:90pt">รับเอกสารวันที่</span></td><td style="font-size:6.3pt"><b><u>ตรวจครั้งที่ 2</u></b> <span style="margin-left:110pt">รับเอกสารวันที่</span></td></tr>
 <tr><td style="font-size:6.3pt">เอกสารส่งงาน <span style="margin-left:40pt">☐ &nbsp;Pass</span><span style="margin-left:60pt">☐ &nbsp;No Pass</span></td><td style="font-size:6.3pt">เอกสารส่งงาน <span style="margin-left:80pt">☐ Pass</span><span style="margin-left:20pt">☐ No Pass</span></td></tr>
 <tr><td style="font-size:6.3pt">หมายเหตุ</td><td style="font-size:6.3pt">หมายเหตุ</td></tr>
 {''.join('<tr><td></td><td></td></tr>' for _ in range(5))}
 <tr><td style="height:20pt;font-size:6.3pt">ผู้ตรวจสอบ<span style="margin-left:200pt">วันที่</span></td><td style="font-size:6.3pt">ผู้ตรวจสอบ<span style="margin-left:150pt">วันที่</span></td></tr>
 <tr><td style="height:20pt;font-size:6.3pt">ผู้เห็นชอบ<span style="margin-left:203pt">วันที่</span></td><td style="font-size:6.3pt">ผู้อนุมัติ<span style="margin-left:157pt">วันที่</span></td></tr>
</table>
<div style="text-align:right;font-size:6pt;margin-top:6pt">Prepare by : Outside Plant Division on 10 April 2008</div>
</div></div>"""

# ================================================================ สรุปผลการตรวจรับ


INSPECT_L = [("1", "การติดตั้งเคเบิลภายนอก", None)] + [(None, t, k) for t, k in [
    ("1.1 ความปลอดภัยห่างจากถนน / ทางเดินเท้า", 1), ("1.2 ความปลอดภัยห่างจากสายไฟฟ้า", 1), ("1.3 ระดับความสูงของเคเบิล", 1),
    ("1.4 ความตึงของสายเคเบิล", 1), ("1.5 ลักษณะการจับยึดสาย", 1), ("1.6 การขดลูปของสายเคเบิล", 1),
    ("1.7 การจัดเคเบิลเส้นเก่าให้เรียบร้อย", 1), ("1.8 การติดตั้งเคเบิลลงท่อ Riser", 0), ("1.9 ติดตั้งไม้กางเขนได้ถูกต้อง", 0),
    ("1.10 การทำ Pull Off", 0), ("1.11 การติดตั้งสายสะพาน", 0), ("1.12 การพันควบของเคเบิลกับสายสะพาน", 0)]] + \
    [("2", "การ Splice ใน Enclosure", None)] + [(None, t, k) for t, k in [
        ("2.1 การจัดเก็บสายเคเบิลและการขดลูป", 1), ("2.2 ตำแหน่งการติดตั้ง Enclosure", 1),
        ("2.3 ป้ายบอกทิศทางของเคเบิล (แผ่นตะกั่ว)", 1), ("2.4 การยึด Enclosure", 1), ("2.5 การติดป้ายบอกลำดับที่ Enclosure (SJ. Label)", 0)]]
INSPECT_R = [("3", "การ Splice ในบ่อพัก", None)] + [(None, t, 0) for t in [
    "3.1 การจัดเก็บสายเคเบิล", "3.2 ตำแหน่งการติดตั้ง Enclosure", "3.3 ป้ายบอกทิศทางของเคเบิล (แผ่นตะกั่ว)", "3.4 การยึด Rack Hook",
    "3.5 การพ่นอักษรบอกลำดับที่และเส้นทางที่ Enclosure"]] + [("4", "การ Termination ภายใน Site", None)] + [(None, t, 1) for t in [
        "4.1 การติดตั้ง ODF และ จุดเข้า", "4.2 การจัดสาย Fiber ภายใน ODF", "4.3 ติดป้ายบอกเลขที่ CORE (Core Marker)",
        "4.4 ป้ายบอกทิศทางของเคเบิล (แผ่นตะกั่ว)", "4.5 ทำความสะอาดภายในสถานี"]] + [("5", "การติดตั้งกราวด์", None)] + [(None, t, 0) for t in [
            "5.1 ตำแหน่งติดตั้ง กราวด์", "5.2 การเชื่อมกราวด์กับสายสะพาน", "5.3 ผล Test  กราวด์รวม (Ground System)",
            "5.4 ผล Test   กราวด์  ร็อด (แท่งกราวด์)", "5.5 การเชื่อมต่อกันของสายสะพาน"]] + [("6", "ผลทดสอบ OTDR", None)] + \
    [(None, "6.1 Connector loss", 1), (None, "6.2 Splice  Loss", 1)]


def page_inspection(job, company, logo_l, logo_r):
    f = job["fields"]
    sub, insp, mgr = _sig(f, "sub"), _sig(f, "insp"), _sig(f, "mgr")
    n = max(len(INSPECT_L), len(INSPECT_R))

    def cells(item):
        if item is None:
            return '<td></td><td></td><td></td><td></td>'
        no, text, k = item
        if no:
            return f'<td class="c cy">{no}</td><td class="c b cy">{text}</td><td></td><td></td>'
        t = "/" if k else ""
        return f'<td></td><td>{text}</td><td class="c">{t}</td><td class="c">{t}</td>'
    body = []
    for i in range(n):
        L = INSPECT_L[i] if i < len(INSPECT_L) else None
        R = INSPECT_R[i] if i < len(INSPECT_R) else None
        if L is None:
            Lc = (f'<td colspan="4" rowspan="{n - i}" class="c b cy" style="font-size:7.5pt">ผ่านให้ขีด ✓, ไม่ผ่านให้ขีด ✗ ในช่อง , ขีด - คือไม่ได้ใช้ในการติดตั้งเส้นทางนี้</td>'
                  if i == len(INSPECT_L) else "")
        else:
            Lc = cells(L)
        body.append(f"<tr>{Lc}{cells(R)}</tr>")
    cm = "".join('<tr><td style="height:12.5pt"></td><td></td><td></td><td></td><td></td><td></td></tr>' for _ in range(12))
    return f"""<div class="pg"><div class="abs" style="left:47pt;top:69pt;width:549pt;font-size:6.3pt">
<table class="g"><colgroup><col style="width:21pt"><col style="width:189pt"><col style="width:39pt"><col style="width:56pt"><col style="width:22pt"><col style="width:155pt"><col style="width:37pt"><col style="width:30pt"></colgroup>
 <tr><td colspan="8" class="cy" style="height:21pt"><div style="display:flex;align-items:center;height:20pt">
   <div style="width:80pt;padding-left:6pt">{f'<img src="{_uri(logo_l)}" style="height:15pt">' if logo_l else ''}</div>
   <div style="flex:1;text-align:center;font-size:7.5pt;font-weight:700">สรุปผลการตรวจรับการติดตั้ง OPTICAL FIBER CABLE</div>
   <div style="width:80pt;text-align:right;padding-right:4pt">{f'<img src="{_uri(logo_r)}" style="height:15pt">' if logo_r else ''}</div></div></td></tr>
 <tr><td colspan="2" class="c cy" style="height:34pt">บริษัทผู้รับเหมา (CONTRACTOR)</td><td class="c cy">ชื่อ Link :</td>
   <td colspan="5" class="c" style="font-size:9.5pt">{esc(job['link_name'])}</td></tr>
 <tr><td colspan="2" class="c" style="height:30pt">{esc(company.get('name'))}</td><td class="c cy">Project :</td>
   <td colspan="5" style="padding:0"><table style="width:100%;height:30pt;border-collapse:collapse;table-layout:fixed"><tr>
     <td style="width:96pt;border:none;border-right:0.6pt solid #000;font-size:5.8pt;white-space:normal" class="c">{esc(f.get('inspect_project'))}</td>
     <td style="width:38pt;border:none;border-right:0.6pt solid #000" class="c b cy">ชนิด OFC</td>
     <td style="width:90pt;border:none;border-right:0.6pt solid #000;font-size:5.5pt;white-space:normal" class="c">({esc(f.get('ofc'))})</td>
     <td style="width:38pt;border:none;border-right:0.6pt solid #000" class="c cy">SAP No.</td>
     <td style="border:none" class="c">{esc(f.get('sap_no'))}</td></tr></table></td></tr>
 <tr><td class="c cy">ลำดับ</td><td class="c b cy">รายละเอียด</td><td class="c cy" style="font-size:5.3pt;line-height:1.2;white-space:normal">ผู้รับเหมา<br>Sub-Contractor</td><td class="c cy" style="font-size:5.3pt;line-height:1.2;white-space:normal">เจ้าหน้าที่<br>Contractor</td>
   <td class="c cy">ลำดับ</td><td class="c b cy">รายละเอียด</td><td class="c cy" style="font-size:4.6pt;line-height:1.2;white-space:normal">ผู้รับเหมา<br>Sub-Contractor</td><td class="c cy" style="font-size:4.6pt;line-height:1.2;white-space:normal">เจ้าหน้าที่<br>Contractor</td></tr>
 {''.join(body)}
</table>
<table class="g"><colgroup><col style="width:38pt"><col></colgroup>
 <tr><td class="b" style="vertical-align:top;height:16pt;border-bottom:none">หมายเหตุ</td><td style="border-top:none;border-right:none"></td></tr>
 {''.join('<tr><td style="border-top:none;border-bottom:none;height:15pt"></td><td></td></tr>' for _ in range(4))}
</table>
<table class="g"><colgroup><col style="width:21pt"><col style="width:285pt"><col style="width:42pt"><col style="width:60pt"><col style="width:58pt"><col style="width:83pt"></colgroup>
 <tr><td colspan="6" class="c cy">Comment &nbsp;Sheet</td></tr>
 <tr><td rowspan="2" class="c cy">ลำดับที่</td><td rowspan="2" class="c b cy">รายละเอียด</td><td class="c cy">กำหนดเสร็จ</td><td colspan="2" class="c b cy">ตรวจงานแก้ไข</td><td rowspan="2" class="c b cy">ผู้ตรวจสอบ</td></tr>
 <tr><td class="c cy">วันที่</td><td class="c cy">ผลการตรวจ</td><td class="c cy">วันที่</td></tr>
 {cm}
</table>
<table class="g sig" style="margin-top:2pt"><colgroup><col style="width:38pt"><col><col><col><col></colgroup>
 <tr><td class="c cy" style="height:22pt">สำหรับ</td><td class="c b cy">Sub Contractor</td><td class="c b cy">Name of Contractor  Inspector</td><td class="c b cy">Name of Contractor Manager</td><td class="c b cy">True  Inspector</td></tr>
 <tr><td class="r" style="height:26pt">ลายเซ็น :</td><td class="c">{sub['img']}</td><td class="c">{insp['img']}</td><td class="c">{mgr['img']}</td><td></td></tr>
 <tr><td class="r" style="height:22pt">ชื่อ :</td><td class="c" style="font-size:9pt">{esc(sub['name'])}</td><td class="c" style="font-size:9pt">{esc(insp['name'])}</td><td class="c" style="font-size:9pt">{esc(mgr['name'])}</td><td></td></tr>
 <tr><td class="r" style="height:20pt">วันที่ :</td><td class="c">{esc(sub['date'])}</td><td class="c">{esc(insp['date'])}</td><td class="c">{esc(mgr['date'])}</td><td></td></tr>
</table></div></div>"""

# ================================================================ Link budget


def _n(v, d=2):
    return "" if v is None else f"{v:.{d}f}"


def page_linkbudget(job, logo_l):
    f = job["fields"]
    s1, s2 = sites_from_link(job["link_name"])
    lb = link_budget(f)
    img = f'<img src="{_uri(logo_l)}" style="height:22pt;position:absolute;right:14pt;top:10pt">' if logo_l else ""
    hl = "background:#00FFFF"

    def line(label, val, unit, bg="", note=""):
        return (f'<tr><td style="width:240pt">{label}</td><td style="width:20pt" class="c">=</td>'
                f'<td style="width:50pt;border-bottom:0.6pt solid #000;{bg}" class="c">{val}</td><td style="width:40pt">&nbsp;{unit}</td>'
                f'<td style="color:#C00;font-weight:700;font-size:7pt">{note}</td></tr>')
    p1 = "".join([
        line("L &nbsp;= ความยาวของ Optical Fiber", _n(lb["L"], 3), "Km.", hl, "ระยะจาก OTDR"),
        line("aF = ค่า Attenuation ของ fiber", _n(lb["aF"]), "dB/Km."),
        line("NE = จำนวนของจุด splice ไม่รวมใน ODF", _n(lb["NE"]), "Point", hl),
        line("aE &nbsp;= ค่าเฉลี่ยของ attenuation ของจุด Splice รวมกันทุกจุด", _n(lb["aE"]), "dB"),
        line("NC = จำนวนของ Connector", _n(lb["NC"]), "Point", hl),
        line("aC &nbsp;= ค่าเฉลี่ยของ attenuation ต่อ connector รวม Splice ใน ODF", _n(lb["aC"]), "dB")])
    p2 = "".join([
        line("ค่า attenuation โดยรวมของ Cable ทั้งเส้นทาง (LxaF)", _n(lb["cable"]), "dB"),
        line("ค่า attenuation โดยรวมของจุด Splice ทั้งเส้นทาง (NExaE)", _n(lb["splice"]), "dB"),
        line("ค่า attenuation โดยรวมของ Connector ทั้งเส้นทาง (NCxaC)", _n(lb["conn"]), "dB")])
    p3 = "".join([
        line("ค่า attenuation โดยรวมของเส้นทางโดยประมาณ (จากการคำนวณ)", _n(lb["total"]), "dB", "background:#FFFF00"),
        line("ค่า attenuation โดยรวมของเส้นทางเมื่อทดสอบด้วยเครื่อง OTDR (Max.)", _n(lb["otdr_max"]), "dB", hl),
        line("ค่า attenuation โดยรวมของเส้นทางเมื่อทดสอบด้วยเครื่อง OTDR (Min.)", _n(lb["otdr_min"]), "dB", hl)])
    wl = esc(f.get("lb_wavelength") or "1550")
    return f"""<div class="pg"><div class="frame" style="left:109pt;top:61pt;width:466pt;height:655pt;font-size:7.6pt">
  <div style="height:45pt;background:#00FFFF;border-bottom:1.2pt solid #000;position:relative">
    <div style="position:absolute;left:4pt;top:14pt;font-weight:700;font-size:9pt">8.2 OPTICAL FIBER LINK BUDGET</div>{img}</div>
  <div style="text-align:right"><span style="background:#00B0F0;font-weight:700;font-size:6pt;padding:2pt 8pt;display:inline-block">PAT Doc. Version: 6.0</span></div>
  <table style="margin:4pt 0 0 60pt;border-collapse:collapse">
    <tr><td style="text-align:right;padding:2pt 6pt">Link : Name of station &nbsp;A :</td><td style="border-bottom:0.6pt solid #000;width:200pt">{esc(s2)}</td></tr>
    <tr><td style="text-align:right;padding:2pt 6pt">Link : Name of station &nbsp;B :</td><td style="border-bottom:0.6pt solid #000">{esc(s1)}</td></tr>
    <tr><td style="text-align:right;padding:2pt 6pt">Route Name :</td><td style="border-bottom:0.6pt solid #000">{esc(s2)} - {esc(s1)}</td></tr></table>
  <div style="border:0.8pt solid #000;margin:14pt 60pt;padding:4pt;text-align:center">ค่า Attenuation โดยรวม (dB) = ( L x aF ) + ( NE X aE ) + ( NC X aC )</div>
  <table style="margin-left:62pt;border-collapse:collapse;line-height:1.9">{p1}</table><br>
  <table style="margin-left:62pt;border-collapse:collapse;line-height:1.9">{p2}</table><br>
  <table style="margin-left:30pt;border-collapse:collapse;line-height:1.9">{p3}</table>
  <div style="margin:16pt 0 0 96pt;line-height:1.9">2) กรณี Wavelength เป็น 1550 nm. ค่า Attenuation (aF) = 0.20dB/Km.<br>3) ผล Test OTDR ต้อง Test มาไม่เกิน 1 เดือนก่อนส่งงาน</div>
  <div style="margin:4pt 0 0 30pt;font-weight:700;text-decoration:underline">หมายเหตุ :</div>
  {''.join(f'<div style="border-bottom:0.6pt solid #000;height:19pt;margin:0 30pt;text-align:center;padding-top:5pt">{t}</div>' for t in [esc(f.get('lb_note') or ('Wavelength ' + wl + ' nm')), '', '', '', ''])}
</div></div>"""

# ================================================================ Deficiency


def page_deficiency(job, company, logo_l):
    f = job["fields"]
    mgr = _sig(f, "mgr")
    rows = "".join("<tr><td style='height:15.6pt'></td><td></td><td></td><td></td></tr>" for _ in range(27))
    img = f'<img src="{_uri(logo_l)}" style="height:13pt;position:absolute;right:16pt;top:2pt">' if logo_l else ""
    subname = f.get("def_subcon") or company.get("name")
    return f"""<div class="pg"><div class="abs" style="left:41pt;top:39pt;width:536pt;font-size:6pt">
  <div style="height:6pt;background:#FFFF00;position:relative">{img}</div>
  <div class="c b" style="font-size:8.5pt;line-height:2.2;margin-top:6pt">DEFFICIENCY LIST FOR PHYSICAL (ODN)<br>Region {esc(f.get('def_region') or f.get('region'))}<br>{esc(job['link_name'])}<br>
    Date ................./................../.................<br>Sub-Contractor name : {esc(subname)}</div>
  <div class="r b" style="margin:4pt 30pt 6pt 0">PAT Doc. Version <span style="margin-left:24pt">3.0</span></div>
  <table class="g"><colgroup><col style="width:35pt"><col style="width:46pt"><col><col style="width:132pt"></colgroup>
    <tr><th class="c b">Item</th><th class="c b">Category</th><th class="c b">Description</th><th class="c b">Clear / Acknowledgement</th></tr>{rows}</table>
  <div class="b" style="margin-top:2pt">Remark</div>
  <div style="font-size:4.8pt">กรณีมี DEFFICIENCY ให้ถ่ายรูปก่อนการแก้ไข ทุกหัวข้อ เพราะต้องไปใส่ลงใน แบบฟอร์มการแก้ไขข้อบกพร่อง (Deficiencies Clearance Form) &nbsp; หน้า P.32 ด้วย</div>
  <table class="g sig" style="margin-top:6pt;font-size:5.5pt"><colgroup><col style="width:81pt"><col style="width:132pt"><col style="width:132pt"><col></colgroup>
    <tr class="pk"><td class="r b">สำหรับ</td><td class="c b">Contractor</td><td colspan="2" class="c b" style="color:#C00">True Inspector Approve</td></tr>
    <tr class="pk"><td class="r b">Department</td><td class="c b">{esc(company.get('name'))}</td><td class="c b">Online Implementation</td><td class="c b">Network Operation (NOP)</td></tr>
    <tr><td class="r b" style="height:20pt">ลายเซ็น</td><td class="c">{mgr['img']}</td><td></td><td></td></tr>
    <tr><td class="r b" style="height:15pt">ชื่อตัวบรรจง</td><td class="c b">{esc(mgr['name'])}</td><td></td><td></td></tr>
    <tr><td class="r b" style="height:15pt">ชื่อหน่วยงาน</td><td class="c b">{esc(company.get('name'))}</td><td class="c b">Rollout</td><td class="c b">NOP</td></tr>
    <tr><td class="r b" style="height:15pt">วันที่</td><td class="c b">{esc(mgr['date'])}</td><td></td><td></td></tr>
  </table></div></div>"""

# ================================================================ Report OTDR (.sor)


def _trace_svg(sor, w=520, h=230):
    pts = sor["trace"]
    if not pts:
        return ""
    xmax = max(p[0] for p in pts) or 1
    ymax = max(5, (int(max(p[1] for p in pts) / 10) + 1) * 10)
    L, B = 34, 22
    pw, ph = w - L - 8, h - B - 6
    X = lambda x: L + x / xmax * pw
    Y = lambda y: 6 + ph - max(0, y) / ymax * ph
    path = " ".join(f"{X(x):.1f},{Y(y):.1f}" for x, y in pts)
    grid = []
    for v in range(0, ymax + 1, 10):
        grid.append(f'<line x1="{L}" x2="{L + pw}" y1="{Y(v):.1f}" y2="{Y(v):.1f}" stroke="#CCC" stroke-dasharray="2,2" stroke-width="0.5"/>'
                    f'<text x="{L - 3}" y="{Y(v) + 3:.1f}" font-size="7" text-anchor="end">{v:.2f}</text>')
    for k in range(0, int(xmax) + 1):
        grid.append(f'<line y1="6" y2="{6 + ph}" x1="{X(k):.1f}" x2="{X(k):.1f}" stroke="#CCC" stroke-dasharray="2,2" stroke-width="0.5"/>'
                    f'<text x="{X(k):.1f}" y="{h - 8}" font-size="7" text-anchor="middle">{k}</text>')
    marks = []
    for e in sor["events"]:
        x = X(e["distance"])
        yv = next((p[1] for p in pts if p[0] >= e["distance"]), 0)
        marks.append(f'<line x1="{x:.1f}" x2="{x:.1f}" y1="{Y(yv) - 8:.1f}" y2="{Y(yv) + 8:.1f}" stroke="#C00" stroke-width="0.8"/>'
                     f'<text x="{x + 2:.1f}" y="{Y(yv) + 16:.1f}" font-size="7">{e["no"]}</text>')
    return (f'<svg width="{w}pt" height="{h}pt" viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">'
            f'<text x="4" y="4" font-size="7">dB</text><text x="{w - 14}" y="{h - 8}" font-size="7">km</text>{"".join(grid)}'
            f'<polyline points="{path}" fill="none" stroke="#000" stroke-width="0.7"/>{"".join(marks)}'
            f'<rect x="{L}" y="6" width="{pw}" height="{ph}" fill="none" stroke="#999" stroke-width="0.5"/></svg>')


def page_otdr(sor):
    def d3(v):
        return "- - -" if v in (None, 0, 0.0) else f"{v:.3f}"
    rows = []
    for e in sor["events"]:
        loss = "- - -" if e["type"] in ("Begin", "End") else f"{e['loss']:.3f}"
        refl = "- - -" if not e["refl"] else f"{e['refl']:.3f}"
        att = "- - -" if e["no"] == 1 else f"{e['slope']:.3f}"
        rows.append(f'<tr><td>{e["no"]}&nbsp; {e["type"]}</td><td>{e["distance"]:.3f}</td><td>{loss}</td><td>{refl}</td><td>{att}</td></tr>')
    sec = lambda t: (f'<div style="display:flex;align-items:center;gap:6pt;margin:10pt 0 4pt;font-size:8pt;color:#333">'
                     f'<div style="flex:1;border-top:0.6pt solid #999"></div>{t}<div style="flex:1;border-top:0.6pt solid #999"></div></div>')
    rng = f"{sor['range']:.3f} km" if sor.get("range") else ""
    return f"""<div class="pg"><div class="abs" style="left:46pt;top:40pt;width:520pt;font-size:8pt;line-height:1.45">
  <div class="c b" style="font-size:15pt;margin-bottom:6pt">Report OTDR</div>{sec('Parameters')}
  <table style="width:100%;border-collapse:collapse;font-size:8pt"><tr><td style="width:50%;vertical-align:top">
    Begin: {esc(sor['begin'])}<br>Cable: {esc(sor['cable'])}<br>Range: {rng}<br>Wavelength: {esc(sor['wavelength'])} nm (Singlemode)<br>
    Loss Threshold: {esc(sor['loss_thr'])}<br>Date: {esc(sor['date'])}<br>OTDR: {esc(sor['otdr'])} &nbsp; S/N: {esc(sor['otdr_sn'])}<br>
    Module: {esc(sor['module'])} S/N: {esc(sor['module_sn'])}<br>Comment: {esc(sor['comment'])}</td>
    <td style="vertical-align:top">End: {esc(sor['end'])}<br>Fiber: {esc(sor['fiber'])}<br>Pulse Width: {esc(sor['pulse'])}<br>
    Refractive index: {sor['index'] or ''}<br>Refl. Threshold: {esc(sor['refl_thr'])}<br>File: {esc(sor['file'])}</td></tr></table>
  {sec('Summary')}
  <table style="width:100%;font-size:8pt"><tr><td style="width:50%">Fiber Lenght: {d3(sor['length_km'])} km<br>Attenuation: {d3(sor['atten'])} dB/km</td>
    <td style="vertical-align:top">Total Loss: {d3(sor['total_loss'])} dB</td></tr></table>
  {sec('Trace OTDR')}{_trace_svg(sor)}{sec('Event Table')}
  <table style="width:100%;border-collapse:collapse;font-size:8pt" class="ev">
    <tr style="border-bottom:0.6pt solid #000"><td>№ &nbsp;Type</td><td>Distance, km</td><td>Loss, dB</td><td>Reflection, dB</td><td>Attenuation, dB/km</td></tr>
    {''.join(rows)}</table></div></div>"""


def render_html(pages_html):
    doc = (f"<html><head><meta charset='utf-8'><style>{BASE_CSS}"
           ".ev td{padding:3pt 0;border-bottom:0.4pt solid #DDD}</style></head><body>" + "".join(pages_html) + "</body></html>")
    return HTML(string=doc, base_url="/").write_pdf()

# ================================================================ Google Sheet PDF


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


FORM_KEYS = ("PULSEWIDTH", "MATERIALNO", "BALANCECABLE", "รื้อถอน", "PROJECTNAME")
SKIP_KEYS = ("FINALBOQ", "LINKBUDGET", "DEFFICIENCY", "DEFICIENCY")


def detect_sheet_pages(pdf_bytes):
    """หาหน้า Loss OTDR / ใบสรุปเคเบิลรื้อถอน / Balance Cable & Material ในไฟล์ PDF จาก Google Sheet"""
    r = PdfReader(io.BytesIO(pdf_bytes))
    forms = []
    for i, pg in enumerate(r.pages):
        try:
            t = re.sub(r"\s+", "", (pg.extract_text() or "").upper())
        except Exception:
            t = ""
        if any(k in t for k in SKIP_KEYS):
            continue
        if any(k in t for k in FORM_KEYS):
            forms.append(i)
    return {"forms": ",".join(str(x + 1) for x in forms), "count": len(r.pages), "v": 2}

# ================================================================ build


def build_docs(job, company, pages, photos_by_slot, logo_l, logo_r, sheet_pdf=None, sheet_cfg=None, sors=None, asbuilt_pdfs=()):
    """คืน (pat_bytes, asbuilt_bytes)
    PAT     = ทุกหน้า + Report OTDR ท้ายเล่ม
    AS-BUILT = ทุกหน้า (ไม่รวม OTDR) + แบบ As-Built + Report OTDR ท้ายเล่ม"""
    sheet_cfg = sheet_cfg or {}
    sors = sors or []
    reader = PdfReader(io.BytesIO(sheet_pdf)) if sheet_pdf else None
    n = len(reader.pages) if reader else 0
    body = PdfWriter()
    buf = []

    def flush():
        if buf:
            for p in PdfReader(io.BytesIO(render_html(buf))).pages:
                body.add_page(p)
            buf.clear()

    for page in pages:
        k = page["kind"]
        if k == "cover":
            buf.append(page_cover(job, company, logo_l, logo_r))
        elif k == "checklist":
            buf.append(page_checklist(job, logo_l, logo_r))
        elif k == "inspection":
            buf.append(page_inspection(job, company, logo_l, logo_r))
        elif k == "sheet":
            if reader:
                flush()
                for i in parse_ranges(sheet_cfg.get("forms", ""), n):
                    body.add_page(reader.pages[i])
        elif k == "photos":
            buf.append(page_photos(page, photos_by_slot, logo_l, logo_r))
        elif k == "single":
            buf.append(page_single(page, photos_by_slot, logo_l, logo_r))
        elif k == "linkbudget":
            buf.append(page_linkbudget(job, logo_l))
        elif k == "deficiency":
            buf.append(page_deficiency(job, company, logo_l))
    flush()
    otdr_pdf = render_html([page_otdr(s) for s in sors]) if sors else None

    def finish(extra):
        w = PdfWriter()
        for p in body.pages:
            w.add_page(p)
        for b in extra:
            for p in PdfReader(io.BytesIO(b)).pages:
                w.add_page(p)
        out = io.BytesIO()
        w.write(out)
        return out.getvalue()
    pat = finish([otdr_pdf] if otdr_pdf else [])
    asb = finish(list(asbuilt_pdfs) + ([otdr_pdf] if otdr_pdf else []))
    return pat, asb


def count_pages(pdf_bytes):
    return len(PdfReader(io.BytesIO(pdf_bytes)).pages)
