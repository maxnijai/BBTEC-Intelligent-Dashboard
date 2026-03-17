# BBTEC Intelligent Dashboard — Auto-Update via Google Drive + GitHub Actions

import io
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path

import openpyxl
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

GDRIVE_FOLDER_ID = "188bv1FhdU2A64wjJjkntkKFIGZG18nMJ"

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "dashboard_output"
SERVICE_ACCOUNT_FILE = SCRIPT_DIR / "service_account.json"
LAST_TRACKER = SCRIPT_DIR / "last_processed.txt"
TPL_FILE = SCRIPT_DIR / "templates" / "tpl_component.txt"

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

AF_ORDER = [
    "6) Within SLA",
    "5) OverSLA : < 1 day",
    "4) OverSLA : < 3 days",
    "3) OverSLA : < 7 days",
    "2) OverSLA : < 30 days",
    "1) OverSLA : > 30 days",
]


def haversine(lat1, lon1, lat2, lon2):
    r = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return r * 2 * atan2(sqrt(a), sqrt(1 - a))


def get_drive_svc():
    if not SERVICE_ACCOUNT_FILE.exists():
        raise FileNotFoundError(f"service_account.json not found: {SERVICE_ACCOUNT_FILE}")

    creds = service_account.Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_FILE),
        scopes=SCOPES,
    )
    return build("drive", "v3", credentials=creds)


def find_latest_gdrive():
    svc = get_drive_svc()
    res = svc.files().list(
        q=(
            f"'{GDRIVE_FOLDER_ID}' in parents and "
            "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' "
            "and trashed=false"
        ),
        orderBy="modifiedTime desc",
        pageSize=1,
        fields="files(id,name,modifiedTime)",
    ).execute()

    files = res.get("files", [])
    if not files:
        return None, 0, None

    f = files[0]
    mt = datetime.fromisoformat(f["modifiedTime"].replace("Z", "+00:00")).timestamp()
    print("  GDrive:", f["name"])
    return f, mt, svc


def download_gdrive(svc, fid, fname):
    OUTPUT_DIR.mkdir(exist_ok=True)
    fp = OUTPUT_DIR / fname

    req = svc.files().get_media(fileId=fid)
    fh = io.BytesIO()
    dl = MediaIoBaseDownload(fh, req)

    done = False
    while not done:
        _, done = dl.next_chunk()

    fh.seek(0)
    with open(fp, "wb") as f:
        f.write(fh.read())

    return str(fp)


def find_best_file():
    gf, _, svc = find_latest_gdrive()
    if gf:
        return download_gdrive(svc, gf["id"], gf["name"]), gf["name"]
    return None, None


def process_excel(fp):
    print("  Processing:", os.path.basename(fp))
    wb = openpyxl.load_workbook(str(fp), read_only=True, data_only=True)

    if "data" not in wb.sheetnames:
        wb.close()
        raise KeyError("Sheet 'data' not found in Excel file")

    ws = wb["data"]
    all_rows = []

    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0:
            continue
        if row[0] is None:
            continue

        reg = str(row[11]) if row[11] else ""
        if reg not in ("NOR1", "NOR2"):
            continue

        owner = str(row[12]) if row[12] else ""
        if "UN-NPMECT-RF" in owner:
            continue

        sev = str(row[7] or "")
        af = str(row[22] or "")
        prov = str(row[36]) if row[36] and row[36] != "None" else ""
        district = str(row[38]) if row[38] and row[38] != "None" else ""
        site = str(row[35] or "")
        tid = str(row[3] or "")
        subject = str(row[8])[:80] if row[8] else ""
        aging = round(float(row[21] or 0), 2)
        sowner = owner.split("BBT-")[1] if "BBT-" in owner else owner
        bookmark = str(row[25] or "")
        cat = str(row[10] or "")
        itime = str(row[0] or "")
        creation = str(row[5] or "")
        cls_full = str(row[19] or "")

        parts = [p.strip() for p in cls_full.split("  ") if p.strip()]
        vendor, problem, root = "Other", "Other", "Other"

        if len(parts) >= 2:
            vp = parts[1]
            for v, k in [("ERICSSON", "Ericsson"), ("NOKIA", "Nokia"), ("HUAWEI", "Huawei")]:
                if v in vp:
                    vendor = k
                    break
            for pt in ["ROUTE SITE DOWN", "SITE UP/DOWN", "SITE DOWN", "CELL DOWN"]:
                if pt in vp:
                    problem = pt
                    break

        if len(parts) >= 3:
            root = parts[2]

        subimpact = 0
        try:
            subimpact = int(float(str(row[13]))) if row[13] and row[13] != "None" else 0
        except Exception:
            pass

        lat = row[23]
        lon = row[24]
        flat, flon, hc = 0, 0, False
        if lat and lat != "None" and lon and lon != "None":
            try:
                flat = round(float(lat), 4)
                flon = round(float(lon), 4)
                hc = True
            except Exception:
                pass

        status = str(row[4] or "")
        ciname = str(row[9] or "")
        target = str(row[6] or "")
        over_sla = str(row[2] or "")
        plan_close = str(row[18]) if row[18] and row[18] != "None" else ""
        ext_sys = str(row[30] or "")

        pri = 2
        now = datetime.now()
        ref_time = datetime(now.year, now.month, now.day, 1, 15, 0)
        if ref_time <= now:
            ref_time = ref_time + timedelta(days=1)

        if target and target != "None":
            try:
                dt = datetime.strptime(target[:19], "%Y-%m-%d %H:%M:%S")
                diff_h = (ref_time - dt).total_seconds() / 3600
                if diff_h > 24:
                    pri = 0
                elif diff_h > 0:
                    pri = 1
                else:
                    pri = 2
            except Exception:
                pass

        all_rows.append(
            {
                "sev": sev,
                "reg": reg,
                "sowner": sowner,
                "vendor": vendor,
                "problem": problem,
                "root": root,
                "af": af,
                "aging": aging,
                "prov": prov,
                "bookmark": bookmark,
                "cat": cat,
                "lat": flat,
                "lon": flon,
                "hc": hc,
                "itime": itime,
                "district": district,
                "subimpact": subimpact,
                "creation": creation,
                "site": site,
                "cls": cls_full,
                "pri": pri,
                "ticket": {
                    "tid": tid,
                    "sev": sev,
                    "reg": reg,
                    "af": af,
                    "aging": aging,
                    "sowner": sowner,
                    "prov": prov,
                    "dist": district,
                    "status": status,
                    "cat": cat,
                    "problem": problem,
                    "root": root,
                    "site": site,
                    "ci": ciname,
                    "sub": subimpact,
                    "created": creation[:16],
                    "target": target[:16],
                    "over_sla": over_sla,
                    "plan": plan_close[:16],
                    "ext": ext_sys,
                    "bookmark": bookmark,
                    "lat": flat,
                    "lon": flon,
                    "subj": subject,
                    "cls": cls_full,
                    "pri": pri,
                },
            }
        )

    wb.close()

    if not all_rows:
        print("  No data")
        return None

    def cluster_5km(rows):
        tix = [r for r in rows if r["hc"]]
        if not tix:
            return []

        used = set()
        clusters = []

        for idx in sorted(range(len(tix)), key=lambda i: -tix[i]["aging"]):
            if idx in used:
                continue

            t = tix[idx]
            members = []
            for idx2 in range(len(tix)):
                if idx2 in used:
                    continue
                if haversine(t["lat"], t["lon"], tix[idx2]["lat"], tix[idx2]["lon"]) <= 5:
                    members.append(idx2)
                    used.add(idx2)

            if len(members) >= 2:
                ctix = [tix[j] for j in members]
                sites = list(set(t2["site"] for t2 in ctix if t2["site"]))
                af_c = Counter(t2["af"] for t2 in ctix)
                tkt_det = [
                    {
                        "tid": t2["ticket"]["tid"],
                        "site": t2["site"],
                        "subj": t2["ticket"]["subj"],
                        "aging": t2["aging"],
                        "af": t2["af"],
                        "sev": t2["sev"],
                        "lat": t2["lat"],
                        "lon": t2["lon"],
                        "cls": t2["cls"],
                        "pri": t2["pri"],
                    }
                    for t2 in sorted(ctix, key=lambda x: -x["aging"])
                ]
                clusters.append(
                    {
                        "lat": round(sum(t2["lat"] for t2 in ctix) / len(ctix), 4),
                        "lon": round(sum(t2["lon"] for t2 in ctix) / len(ctix), 4),
                        "count": len(ctix),
                        "sites": len(sites),
                        "site_names": sites[:8],
                        "prov": ctix[0]["prov"],
                        "dist": ctix[0]["district"],
                        "reg": ctix[0]["reg"],
                        "avg": round(sum(t2["aging"] for t2 in ctix) / len(ctix), 2),
                        "max": round(max(t2["aging"] for t2 in ctix), 2),
                        "sowners": list(set(t2["sowner"] for t2 in ctix)),
                        "tkt": tkt_det,
                        **{k: af_c.get(k, 0) for k in AF_ORDER},
                    }
                )

        clusters.sort(key=lambda x: -x["count"])
        return clusters[:30]

    def build_weather(rows):
        hourly = defaultdict(lambda: {"total": 0, "power": 0, "provs": Counter()})
        for r in rows:
            c = r["creation"]
            if c and ("2026-03-11" in c or "2026-03-12" in c):
                hr = c[11:13] if len(c) > 12 else "00"
                key = c[:10] + " " + hr + ":00"
                hourly[key]["total"] += 1
                if "POWER" in r["root"]:
                    hourly[key]["power"] += 1
                if r["prov"]:
                    hourly[key]["provs"][r["prov"]] += 1

        timeline = [
            {
                "time": k,
                "total": d["total"],
                "power": d["power"],
                "provs": ",".join(p + "(" + str(c) + ")" for p, c in d["provs"].most_common(3)),
            }
            for k, d in sorted(hourly.items())
        ]
        peak = sorted(timeline, key=lambda x: -x["total"])[:5]
        t11 = sum(h["total"] for k, h in hourly.items() if "03-11" in k)
        t12 = sum(h["total"] for k, h in hourly.items() if "03-12" in k)
        p11 = sum(h["power"] for k, h in hourly.items() if "03-11" in k)
        p12 = sum(h["power"] for k, h in hourly.items() if "03-12" in k)
        return {
            "timeline": timeline,
            "peak": peak,
            "summary": {
                "total_11mar": t11,
                "total_12mar": t12,
                "power_11mar": p11,
                "power_12mar": p12,
                "peak_hour": peak[0]["time"] if peak else "",
                "peak_count": peak[0]["total"] if peak else 0,
                "storm_total": t11 + t12,
                "storm_power": p11 + p12,
            },
        }

    def build_tab(rows):
        if not rows:
            return None

        total = len(rows)
        itime = max((r["itime"] for r in rows), default="")
        coords = [
            {
                "lat": r["lat"],
                "lon": r["lon"],
                "af": r["af"],
                "sev": r["sev"],
                "sowner": r["sowner"],
                "prov": r["prov"],
                "aging": r["aging"],
                "problem": r["problem"],
                "root": r["root"],
                "district": r["district"],
                "subimpact": r["subimpact"],
            }
            for r in rows
            if r["hc"]
        ]

        af_c = Counter(r["af"] for r in rows)
        sev_af = defaultdict(lambda: {k: 0 for k in AF_ORDER})
        sev_t = Counter()
        for r in rows:
            sev_af[r["sev"]][r["af"]] += 1
            sev_t[r["sev"]] += 1

        sev_af_l = [{"name": s, "total": sev_t[s], **sev_af[s]} for s in sorted(sev_t.keys(), key=lambda x: -sev_t[x])]

        sub_v = [r["subimpact"] for r in rows]
        sub_nz = [v for v in sub_v if v > 0]
        sub_sum = {"total": sum(sub_v), "count_nonzero": len(sub_nz)}

        dd = defaultdict(lambda: {"c": 0, "prov": "", "reg": "", "af": Counter(), "as": 0, "st": 0, "sc": 0, "ls": 0, "lo": 0, "cc": 0})
        for r in rows:
            dk = r["district"] or "ไม่ระบุ"
            d = dd[dk]
            d["c"] += 1
            d["prov"] = r["prov"] or d["prov"]
            d["reg"] = r["reg"]
            d["af"][r["af"]] += 1
            d["as"] += r["aging"]
            if r["subimpact"] > 0:
                d["st"] += r["subimpact"]
                d["sc"] += 1
            if r["hc"]:
                d["ls"] += r["lat"]
                d["lo"] += r["lon"]
                d["cc"] += 1

        districts = [
            {
                "name": dk,
                "count": d["c"],
                "prov": d["prov"],
                "reg": d["reg"],
                "avg": round(d["as"] / d["c"], 2),
                "sub_total": d["st"],
                "sub_count": d["sc"],
                "lat": round(d["ls"] / d["cc"], 4) if d["cc"] else 0,
                "lon": round(d["lo"] / d["cc"], 4) if d["cc"] else 0,
                **{k: d["af"].get(k, 0) for k in AF_ORDER},
            }
            for dk, d in sorted(dd.items(), key=lambda x: -x[1]["c"])
        ]

        od = defaultdict(lambda: {"c": 0, "reg": "", "prov": "", "af": Counter(), "as": 0, "sd": 0, "cd": 0, "pf": 0, "ip": 0, "st": 0, "sc": 0})
        for r in rows:
            d = od[r["sowner"]]
            d["c"] += 1
            d["reg"] = r["reg"]
            if not d["prov"] and r["prov"]:
                d["prov"] = r["prov"]
            d["af"][r["af"]] += 1
            d["as"] += r["aging"]
            if r["problem"] == "SITE DOWN":
                d["sd"] += 1
            elif r["problem"] == "CELL DOWN":
                d["cd"] += 1
            if r["root"] == "MAIN AC POWER FAIL":
                d["pf"] += 1
            if "IPRAN" in r["root"]:
                d["ip"] += 1
            if r["subimpact"] > 0:
                d["st"] += r["subimpact"]
                d["sc"] += 1

        owners = [
            {
                "name": o,
                "count": d["c"],
                "reg": d["reg"],
                "prov": d["prov"],
                "avg": round(d["as"] / d["c"], 2),
                "sd": d["sd"],
                "cd": d["cd"],
                "pf": d["pf"],
                "ip": d["ip"],
                "sub_total": d["st"],
                "sub_count": d["sc"],
                **{k: d["af"].get(k, 0) for k in AF_ORDER},
            }
            for o, d in sorted(od.items(), key=lambda x: -x[1]["c"])
        ]

        rs = {}
        for reg in ["NOR1", "NOR2"]:
            rr = [r for r in rows if r["reg"] == reg]
            if not rr:
                continue
            raf = Counter(r["af"] for r in rr)
            rs[reg] = {
                "total": len(rr),
                "avg_aging": round(sum(r["aging"] for r in rr) / len(rr), 2),
                **{k: raf.get(k, 0) for k in AF_ORDER},
            }

        rf = defaultdict(lambda: {k: 0 for k in AF_ORDER})
        rt = Counter()
        for r in rows:
            rf[r["root"]][r["af"]] += 1
            rt[r["root"]] += 1

        root_af = [{"name": rc, "total": rt[rc], **rf[rc]} for rc in sorted(rt.keys(), key=lambda x: -rt[x])]

        cat_c = Counter(r["cat"] for r in rows)

        need_team = []
        for d2 in districts:
            if d2["name"] == "ไม่ระบุ":
                continue
            need_team.append(
                {
                    "name": d2["name"],
                    "prov": d2["prov"],
                    "reg": d2["reg"],
                    "count": d2["count"],
                    "avg": d2["avg"],
                    "score": round(d2["count"] * d2["avg"], 1),
                    "over": d2["count"] - d2.get("6) Within SLA", 0),
                }
            )
        need_team.sort(key=lambda x: -x["score"])

        worst = max(owners, key=lambda x: x["avg"]) if owners else None
        best = min(owners, key=lambda x: x["avg"]) if owners else None
        over30 = af_c.get("1) OverSLA : > 30 days", 0)
        within = af_c.get("6) Within SLA", 0)
        n1 = rs.get("NOR1", {})
        n2 = rs.get("NOR2", {})
        td2 = next((d2 for d2 in districts if d2["name"] != "ไม่ระบุ"), None)
        weather = build_weather(rows)
        tickets = [r["ticket"] for r in rows]
        geo_clusters = cluster_5km(rows)
        pri_c = Counter(r["pri"] for r in rows)

        return {
            "total": total,
            "itime": itime,
            "af": {k: af_c.get(k, 0) for k in AF_ORDER},
            "sev_af": sev_af_l,
            "reg_sum": rs,
            "owners": owners,
            "root_af": root_af,
            "districts": districts[:25],
            "subimpact": sub_sum,
            "sev": dict(Counter(r["sev"] for r in rows).most_common()),
            "cat": dict(cat_c.most_common(10)),
            "ven": dict(Counter(r["vendor"] for r in rows).most_common()),
            "prob": dict(Counter(r["problem"] for r in rows).most_common()),
            "aging_avg": round(sum(r["aging"] for r in rows) / total, 2),
            "aging_max": round(max(r["aging"] for r in rows), 2),
            "coords": coords[:500],
            "need_team": need_team[:5],
            "weather": weather,
            "tickets": tickets,
            "clusters": geo_clusters,
            "priority": {"p0": pri_c.get(0, 0), "p1": pri_c.get(1, 0), "p2": pri_c.get(2, 0)},
            "insight": {
                "within_pct": round(within / total * 100, 1),
                "over30": over30,
                "over30_pct": round(over30 / total * 100, 1) if total else 0,
                "worst": worst["name"] if worst else "-",
                "worst_avg": worst["avg"] if worst else 0,
                "best": best["name"] if best else "-",
                "best_avg": best["avg"] if best else 0,
                "nor1_total": n1.get("total", 0),
                "nor1_over": n1.get("total", 0) - n1.get("6) Within SLA", 0),
                "nor2_total": n2.get("total", 0),
                "nor2_over": n2.get("total", 0) - n2.get("6) Within SLA", 0),
                "top_root": root_af[0]["name"] if root_af else "-",
                "top_root_count": root_af[0]["total"] if root_af else 0,
                "top_dist": td2["name"] if td2 else "-",
                "top_dist_count": td2["count"] if td2 else 0,
                "sub_total": sub_sum["total"],
                "sub_affected": sub_sum["count_nonzero"],
            },
        }

    tabs_def = [
        ("tab1", "📊 ภาพรวม Total", lambda r: True),
        ("tab2", "🔴 SA1-4", lambda r: r["sev"] in ("SA1", "SA2", "SA3", "SA4")),
        ("tab3", "📱 7.MB SA1-4", lambda r: r["bookmark"] == "7.MB with SA1-4"),
        ("tab4", "🌐 4.FBB SA1-4", lambda r: r["bookmark"] == "4.FBB with SA1-4"),
        ("tab5", "⚠️ NW Incident NSA1-2", lambda r: r["bookmark"] == "3. All NW Incident NSA1-2"),
        ("tab6", "📋 NSA3-4", lambda r: r["sev"] in ("NSA3", "NSA4")),
    ]

    result = {}
    for tid, name, filt in tabs_def:
        filtered = [r for r in all_rows if filt(r)]
        td = build_tab(filtered)
        if td:
            result[tid] = {"name": name, "data": td}
            print("  " + tid + ": " + str(td["total"]) + " tkts, " + str(len(td["clusters"])) + " clusters")

    return result


def gen_html(tabs_data, src):
    d_js = json.dumps(tabs_data, ensure_ascii=False)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not TPL_FILE.exists():
        raise FileNotFoundError(f"Template file not found: {TPL_FILE}")

    with open(TPL_FILE, "r", encoding="utf-8") as f:
        component = f.read()

    script_content = component.replace("__DATA_PLACEHOLDER__", d_js)

    html = '<!DOCTYPE html>\n<html lang="th"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">\n'
    html += '<title>BBTEC Intelligent Dashboard</title>\n'
    html += '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Sarabun:wght@400;500;600;700&display=swap" rel="stylesheet">\n'
    html += '<script src="https://unpkg.com/react@18/umd/react.production.min.js"></script>\n'
    html += '<script src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>\n'
    html += '<script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>\n'
    html += '<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:Inter,Sarabun,system-ui,sans-serif}button{font-family:inherit}::-webkit-scrollbar{width:6px}::-webkit-scrollbar-track{background:#1e2430}::-webkit-scrollbar-thumb{background:#3c4556;border-radius:3px}</style>\n'
    html += '</head><body><div id="root"></div>\n'
    html += '<div style="position:fixed;bottom:8px;left:8px;background:rgba(40,50,60,0.8);color:#8894a6;padding:4px 10px;border-radius:6px;font-size:9px;font-family:monospace;z-index:999">Updated: ' + ts + ' | ' + src + '</div>\n'
    html += '<script type="text/babel">\n'
    html += 'const{useState,useMemo,useEffect,useRef}=React;\n'
    html += script_content
    html += '\n</script></body></html>'
    return html


def get_last():
    if LAST_TRACKER.exists():
        return LAST_TRACKER.read_text(encoding="utf-8").strip()
    return ""


def set_last(s):
    LAST_TRACKER.write_text(s, encoding="utf-8")


def check():
    print("\n" + "=" * 50)
    print("Checking... (" + datetime.now().strftime("%H:%M:%S") + ")")
    try:
        fp, fn = find_best_file()
        if not fp:
            print("  No files found")
            return

        sig = fn + "_" + str(os.path.getmtime(fp))
        if sig == get_last():
            print("  Up to date: " + fn)
            return

        tabs_data = process_excel(fp)
        if not tabs_data:
            return

                        OUTPUT_DIR.mkdir(exist_ok=True)
        html = gen_html(tabs_data, fn)
        out = OUTPUT_DIR / "dashboard.html"

        with open(out, "w", encoding="utf-8") as f:
            f.write(html)

        total = sum(t["data"]["total"] for t in tabs_data.values())

        summary = {
            "source_file": fn,
            "total_tickets": total,
            "tabs": len(tabs_data),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        summary_file = OUTPUT_DIR / "summary.json"
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        set_last(sig)

        print("\n  Dashboard updated! -> " + str(out))
        print("  Summary updated! -> " + str(summary_file))
        print("  Total: " + str(total) + " tickets across " + str(len(tabs_data)) + " tabs")

    except Exception as e:
        print("  Error: " + str(e))
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    print("BBTEC Intelligent Dashboard — Auto-Updater")
    print("Output: " + str(OUTPUT_DIR))

    if not TPL_FILE.exists():
        print("ERROR: Template file not found: " + str(TPL_FILE))
        sys.exit(1)

    print("Template: " + str(TPL_FILE) + " (" + str(TPL_FILE.stat().st_size // 1024) + " KB)")
    check()
    print("\nFinished.\n")
