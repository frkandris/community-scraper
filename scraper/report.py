"""Daily summary email: traffic + changes per site (HU / international) + totals.

Sent via Resend (same env vars as the feedback emails: RESEND_API_KEY,
RESEND_FROM; recipient = REPORT_EMAIL or FEEDBACK_EMAIL). One email covers the
previous UTC day. Triggered by the report cron in main.py or manually via
POST /admin/api/send-daily-report.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import structlog

from .db import get_daily_summary, get_traffic_for_day

log = structlog.get_logger()

_ROW = "<tr><td style='padding:4px 12px 4px 0;color:#6A6259'>{label}</td>" \
       "<td align='right' style='padding:4px 8px;font-weight:600'>{hu}</td>" \
       "<td align='right' style='padding:4px 8px;font-weight:600'>{intl}</td>" \
       "<td align='right' style='padding:4px 0 4px 8px;font-weight:700'>{total}</td></tr>"

_METRICS = [
    ("new_communities", "Új közösség"),
    ("changed_communities", "Módosult közösség"),
    ("change_rows", "Mezőváltozás"),
    ("new_venues", "Új helyszín"),
    ("new_persons", "Új személy"),
    ("searches", "Keresett pár"),
    ("pages_scraped", "Letöltött oldal"),
    ("pages_extracted", "AI-feldolgozott oldal"),
]


def build_report_html(day: str, summary: dict, traffic: dict) -> tuple[str, str]:
    """Returns (subject, html)."""
    hu, intl = summary["hu"], summary["intl"]
    totals = summary["totals"]

    def t_site(site: str, key: str) -> int:
        return traffic.get(site, {}).get(key, 0)

    total_new = hu["new_communities"] + intl["new_communities"]
    total_visitors = t_site("kozossegek", "visitors") + t_site("meetapedia", "visitors")
    subject = (f"[közösségek] Napi összefoglaló {day} — "
               f"{total_new} új közösség, {total_visitors} látogató")

    metric_rows = "".join(
        _ROW.format(label=label, hu=hu[k], intl=intl[k], total=hu[k] + intl[k])
        for k, label in _METRICS)

    runs_html = ""
    if summary["runs"]:
        items = []
        for r in summary["runs"]:
            state = "✅" if r["success"] else "❌"
            fails = ""
            if r["search_failed"] or r["extract_failed"]:
                fails = (f" — <span style='color:#B4231F'>hibák: "
                         f"{r['search_failed']} keresés, {r['extract_failed']} oldal"
                         f" (nem cache-elve, újrapróbálva)</span>")
            items.append(
                f"<li style='margin:3px 0'>{state} <b>{r['mode']}</b> · "
                f"{(r['started_at'] or '')[11:16]} UTC · {r['pairs']} pár · "
                f"{r['records']} rekord{fails}</li>")
        runs_html = ("<h3 style='margin:18px 0 6px'>Futások</h3>"
                     f"<ul style='margin:0;padding-left:18px'>{''.join(items)}</ul>")
    else:
        runs_html = "<p style='color:#8C8478'>Nem futott pipeline ezen a napon.</p>"

    html = f"""
<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;color:#1C1917;max-width:640px">
  <h2 style="margin:0 0 2px">Napi összefoglaló — {day}</h2>
  <p style="margin:0 0 16px;color:#8C8478;font-size:13px">közösségek.com + meetapedia.com</p>

  <h3 style="margin:0 0 6px">Látogatók</h3>
  <table style="border-collapse:collapse;font-size:14px">
    <tr style="color:#8C8478;font-size:12px">
      <td style="padding:4px 12px 4px 0"></td>
      <td align="right" style="padding:4px 8px">Látogató</td>
      <td align="right" style="padding:4px 0 4px 8px">Oldalletöltés</td></tr>
    <tr><td style="padding:4px 12px 4px 0">kozossegek.com</td>
      <td align="right" style="padding:4px 8px;font-weight:600">{t_site("kozossegek", "visitors")}</td>
      <td align="right" style="padding:4px 0 4px 8px">{t_site("kozossegek", "pageviews")}</td></tr>
    <tr><td style="padding:4px 12px 4px 0">meetapedia.com</td>
      <td align="right" style="padding:4px 8px;font-weight:600">{t_site("meetapedia", "visitors")}</td>
      <td align="right" style="padding:4px 0 4px 8px">{t_site("meetapedia", "pageviews")}</td></tr>
    <tr style="border-top:1px solid #EAE5DB"><td style="padding:4px 12px 4px 0;font-weight:700">Összesen</td>
      <td align="right" style="padding:4px 8px;font-weight:700">{total_visitors}</td>
      <td align="right" style="padding:4px 0 4px 8px;font-weight:700">{t_site("kozossegek", "pageviews") + t_site("meetapedia", "pageviews")}</td></tr>
  </table>
  <p style="color:#B5ADA0;font-size:11px;margin:4px 0 16px">
    Szerveroldali számláló (botok kiszűrve); a GA4 részletes adataihoz lásd az Analyticset.</p>

  <h3 style="margin:0 0 6px">Változások</h3>
  <table style="border-collapse:collapse;font-size:14px">
    <tr style="color:#8C8478;font-size:12px">
      <td style="padding:4px 12px 4px 0"></td>
      <td align="right" style="padding:4px 8px">Magyar</td>
      <td align="right" style="padding:4px 8px">Nemzetközi</td>
      <td align="right" style="padding:4px 0 4px 8px">Össz</td></tr>
    {metric_rows}
  </table>

  {runs_html}

  <h3 style="margin:18px 0 6px">Állomány</h3>
  <p style="margin:0;font-size:14px">
    Közösségek: <b>{totals["hu"]}</b> magyar + <b>{totals["intl"]}</b> nemzetközi
    = <b>{totals["hu"] + totals["intl"]}</b><br>
    Lefedett (keresett) párok: {totals["covered_pairs_hu"]} magyar +
    {totals["covered_pairs_intl"]} nemzetközi</p>

  <p style="color:#B5ADA0;font-size:11px;margin-top:20px">
    közösségek.com napi riport · <a href="https://kozossegek.com/admin" style="color:#A8512F">admin</a></p>
</div>"""
    return subject, html


async def send_daily_report(db_path: Path, hu_cities: set, day: str | None = None) -> dict:
    """Build and send the report for one UTC day (default: yesterday)."""
    api_key = os.environ.get("RESEND_API_KEY", "")
    recipient = os.environ.get("REPORT_EMAIL", "") or os.environ.get("FEEDBACK_EMAIL", "")
    sender = os.environ.get("RESEND_FROM", "onboarding@resend.dev")
    if not api_key or not recipient:
        log.warning("daily_report_skipped", reason="RESEND_API_KEY or FEEDBACK_EMAIL/REPORT_EMAIL missing")
        return {"ok": False, "error": "RESEND_API_KEY or FEEDBACK_EMAIL/REPORT_EMAIL missing"}

    if day is None:
        day = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    start_iso = f"{day}T00:00:00"
    end_day = (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    end_iso = f"{end_day}T00:00:00"

    summary = get_daily_summary(db_path, start_iso, end_iso, hu_cities)
    traffic = get_traffic_for_day(db_path, day)
    subject, html = build_report_html(day, summary, traffic)

    import resend
    resend.api_key = api_key
    resend.Emails.send({"from": sender, "to": [recipient],
                        "subject": subject, "html": html})
    log.info("daily_report_sent", day=day, to=recipient)
    return {"ok": True, "day": day, "to": recipient, "subject": subject}
