# PAT DocGen — ตัวต้นแบบเฟส 1

ระบบสร้างเอกสาร **PAT** และ **AS-BUILT** (Optical Fiber Cable, Template PAT OFC v3.0) จาก ZIP รูปถ่ายแบบกดครั้งเดียว
รองรับหลายบริษัท มีระบบ Login และบันทึกว่าใครทำ Site ไหนไปแล้ว (Audit log)

## ทำอะไรได้บ้าง

| ขั้น | สิ่งที่ระบบทำให้ |
|---|---|
| สร้างงาน | กรอก Link Name → แยก Site 1 / Site 2 ให้เอง (เช่น `DN_PHE1149`, `D_PHE0020`) |
| Import ZIP / โฟลเดอร์ | แตกไฟล์, หมุนรูป, ย่อขนาด, แปลง HEIC, อ่าน KMZ (ชื่อและลำดับหัวต่อ + พิกัด), วางรูปเข้าช่องอัตโนมัติ |
| จับคู่รูป | 1) ชื่อโฟลเดอร์ 2) OCR พิกัดบนรูป → จุดใกล้สุดใน KMZ 3) แยกรูปหน้า/ใน/เสาของ closure จากสีภาพ 4) เรียงตามเวลาถ่าย |
| จัดรูป | หน้าเดียวเห็นทุกหน้า PDF, ช่องที่ระบบไม่มั่นใจเป็นกรอบสีส้ม, คลิกเปลี่ยนรูป/ใส่ N/A |
| Link Budget | คำนวณ (L×aF)+(NE×aE)+(NC×aC) ให้ และเติม NE จากจำนวนหัวต่อใน KMZ |
| สร้าง PDF | PAT (ปก + ฟอร์มจาก Google Sheet + หน้ารูป + Link Budget + หัวต่อทุกจุด + Deficiency + Topo Design + Planwork) และ AS-BUILT (PAT + แบบ As-Built + Report OTDR) เก็บทุกเวอร์ชัน |
| Workflow | ร่าง → พร้อมส่งตรวจ → รอตรวจ → อนุมัติ / ตีกลับ (มีเหตุผล) — งานที่ส่งตรวจ/อนุมัติแล้วจะล็อกการแก้ไข |
| หลายบริษัท | แต่ละบริษัทเห็นเฉพาะงานตัวเอง, Super Admin (BBTEC) เห็นทุกบริษัท, ตั้งโลโก้/ที่อยู่บนปกแยกบริษัท |
| Audit log | บันทึกทุกการกระทำ: login, สร้างงาน, import, เปลี่ยนรูปช่องไหน, สร้าง PDF, ดาวน์โหลด, ส่งตรวจ, อนุมัติ |

## โครงโฟลเดอร์ใน ZIP ที่ระบบอ่านได้แม่นที่สุด

ใช้โครงเดิมที่ทีมทำอยู่ได้เลย (ชื่อไฟล์รูปตั้งอะไรก็ได้):

```
IMG/
├─ PHE1149/            → รูป Site (รอบที่ 1 = 3 รูปแรกตามเวลา, รอบที่ 2 = 3 รูปถัดไป)
├─ PHE0020/
├─ Closure/
│  ├─ SJ#01/ … SJ#05/  → 3 รูปต่อหัว (หน้า closure / ด้านใน / จุดติดตั้ง — ระบบแยกให้)
│  └─ BJ#01/ … BJ#03/
├─ PlugPort/PHE1149/   → Location + หลัง Plug Port + หลัง Plug Core ODF
├─ PlugPort/PHE0020/
├─ Power/              → Test Power (สูงสุด 3 รูป)
├─ PlanWork/           → Support Planwork (ใช้รูปล่าสุด)
├─ Design.jpg          → Topo Design
└─ *.kmz               → ลำดับหัวต่อ + พิกัด (ใช้เรียงหน้า SJ/BJ และจับคู่รูปที่ไม่ได้อยู่ในโฟลเดอร์)
```

## ช่วงเปลี่ยนผ่าน (เฟส 1)

หน้า **Check list / Comment / Loss OTDR / Material / Balance / Deficiency** ยังใช้จาก Google Sheet เดิม
→ export Google Sheet เป็น PDF (ทั้งไฟล์ก็ได้) แล้วแนบในหน้างาน ระบบจะหาเองว่าหน้าไหนเป็นฟอร์ม, Link Budget, Deficiency (แก้เลขหน้าเองได้)
ส่วนหน้ารูปทั้งหมด ไม่ต้องลากรูปใน Sheet อีกแล้ว

## วิธีรันบน Railway (อยู่ในโฟลเดอร์ `pat-docgen/` ของ repo)
1. New Project → Deploy from GitHub repo → เลือก repo นี้
2. Settings → Source → **Root Directory** = `/pat-docgen`
3. Settings → **Watch Paths** = `/pat-docgen/**` (กันไม่ให้ commit อัตโนมัติของ dashboard ไปสั่ง deploy ใหม่)
4. Add Volume → Mount path `/data`
5. Variables: `SECRET_KEY`, `ADMIN_EMAIL`, `ADMIN_PASSWORD` (บังคับ), `SEED_DEMO=1` + `DEMO_PASSWORD` ถ้าอยากได้บริษัททดลอง
6. Settings → Networking → Generate Domain (port 8080)

## บัญชีเริ่มต้น
- Super Admin = `ADMIN_EMAIL` / `ADMIN_PASSWORD` (สร้างครั้งเดียวตอนเปิดครั้งแรก)
- ถ้า `SEED_DEMO=1`: `admin@demo.local` และ `doc@demo.local` รหัส = `DEMO_PASSWORD`
- โลโก้ / ชื่อบนปก / ที่อยู่ ใส่ในเมนู "บริษัท" หลัง login (ไม่เก็บไว้ในโค้ด เพราะ repo เป็น Public)

## โครงสร้างโค้ด

```
app/
  layout.py     โครงหน้า PAT v3.0 (หน้าไหนมีช่องรูปอะไร) + สูตร Link Budget  ← แก้ที่นี่ถ้า Template เปลี่ยน
  importer.py   แตก ZIP, อ่าน KMZ, OCR พิกัด, จับคู่รูปเข้าช่อง
  pdfgen.py     สร้างหน้า PDF (WeasyPrint, ฟอนต์ Sarabun) และรวมกับ PDF จาก Sheet / As-Built / OTDR
  routes.py     หน้าเว็บทั้งหมด + สิทธิ์ + Audit log
  models.py     ตาราง Company, User, Job, Photo, Attachment, Document, Audit
```

## เฟสถัดไป
- ย้ายหน้าฟอร์ม (Check list, Loss, Material, Balance) มากรอกบนเว็บ → เลิกใช้ Google Sheet
- อ่านไฟล์ OTDR `.sor` โดยตรง → เติมระยะ / Loss / ตาราง Loss และสร้างหน้า Report OTDR เอง
- ลายเซ็นดิจิทัลต่อผู้ใช้ ใส่ลง PDF เมื่อกดอนุมัติ
- แอปมือถือให้ช่างถ่ายตามช่อง และใช้ AI แยกประเภทรูป (แทนการวิเคราะห์สี)

ฟอนต์ Sarabun: SIL Open Font License (`app/static/fonts/OFL-Sarabun.txt`)
