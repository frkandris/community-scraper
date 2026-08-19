"""Daily summary email: traffic + changes per site (HU / international) + totals.

Sent via Resend (same env vars as the feedback emails: RESEND_API_KEY,
RESEND_FROM; recipient = REPORT_EMAIL or FEEDBACK_EMAIL). One email covers the
previous UTC day. Triggered by the report cron in main.py or manually via
POST /admin/api/send-daily-report.
"""
from __future__ import annotations

import html as html_lib
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
    ("searches", "Lekeresett város–téma páros"),
    ("pages_scraped", "Letöltött oldal"),
    ("pages_extracted", "AI-feldolgozott oldal"),
]

_STOCK_METRICS = [
    ("communities", "Közösség"),
    ("venues", "Helyszín"),
    ("persons", "Személy"),
    ("pages_cached", "Cache-elt oldal"),
    ("pages_extracted", "AI-feldolgozott oldal"),
    ("covered_pairs", "Lekeresett város–téma páros"),
]


def fetch_ga4_traffic(day: str) -> dict | None:
    """Visitors per hostname from the GA4 Data API for one day.

    Requires GA4_PROPERTY_ID and GA4_CREDENTIALS_JSON (service-account key JSON
    content) env vars; the SA email must be a Viewer on the GA4 property.
    Returns {site: {"visitors": n, "sessions": n, "pageviews": n}} with sites
    mapped from hostName (kozossegek/meetapedia), or None when not configured
    or on any error — the email then falls back to the server-side counter.
    """
    import json as _json
    property_id = os.environ.get("GA4_PROPERTY_ID", "")
    creds_json = os.environ.get("GA4_CREDENTIALS_JSON", "")
    if not property_id or not creds_json:
        return None
    try:
        import httpx
        from google.oauth2 import service_account
        from google.auth.transport.requests import Request as _GARequest

        info = _json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/analytics.readonly"])
        creds.refresh(_GARequest())

        body = {
            "dateRanges": [{"startDate": day, "endDate": day}],
            "dimensions": [{"name": "hostName"}],
            "metrics": [{"name": "activeUsers"}, {"name": "sessions"},
                        {"name": "screenPageViews"}],
        }
        resp = httpx.post(
            f"https://analyticsdata.googleapis.com/v1beta/properties/{property_id}:runReport",
            json=body, headers={"Authorization": f"Bearer {creds.token}"}, timeout=30.0)
        resp.raise_for_status()
        out: dict = {}
        for row in resp.json().get("rows", []):
            host = (row["dimensionValues"][0]["value"] or "").removeprefix("www.")
            site = ("kozossegek" if "kozossegek" in host
                    else "meetapedia" if "meetapedia" in host else None)
            if not site:
                continue
            vals = [int(v["value"]) for v in row["metricValues"]]
            agg = out.setdefault(site, {"visitors": 0, "sessions": 0, "pageviews": 0})
            agg["visitors"] += vals[0]
            agg["sessions"] += vals[1]
            agg["pageviews"] += vals[2]
        return out
    except Exception as exc:
        log.warning("ga4_fetch_failed", error=str(exc))
        return None


def build_report_html(day: str, summary: dict, traffic: dict,
                      ga4: dict | None = None) -> tuple[str, str]:
    """Returns (subject, html)."""
    hu, intl = summary["hu"], summary["intl"]
    totals = summary["totals"]

    def t_site(site: str, key: str) -> int:
        return traffic.get(site, {}).get(key, 0)

    def g_site(site: str, key: str) -> int:
        return (ga4 or {}).get(site, {}).get(key, 0)

    total_new = hu["new_communities"] + intl["new_communities"]
    if ga4 is not None:
        total_visitors = g_site("kozossegek", "visitors") + g_site("meetapedia", "visitors")
    else:
        total_visitors = t_site("kozossegek", "visitors") + t_site("meetapedia", "visitors")
    subject = (f"[közösségek] Napi összefoglaló {day} — "
               f"{total_new} új közösség, {total_visitors} látogató")

    metric_rows = "".join(
        _ROW.format(label=label, hu=hu[k], intl=intl[k], total=hu[k] + intl[k])
        for k, label in _METRICS)

    # Older summaries have no "stock" — fall back to the totals we do have.
    stock = summary.get("stock") or {
        "hu": {"communities": totals["hu"], "covered_pairs": totals["covered_pairs_hu"]},
        "intl": {"communities": totals["intl"], "covered_pairs": totals["covered_pairs_intl"]},
    }

    def s_site(sc: str, key: str) -> int:
        return stock.get(sc, {}).get(key, 0)

    stock_rows = "".join(
        _ROW.format(label=label, hu=s_site("hu", k), intl=s_site("intl", k),
                    total=s_site("hu", k) + s_site("intl", k))
        for k, label in _STOCK_METRICS)

    # Free-tier AI usage for the day. Its own block because the whole point of
    # the router is that this number stays inside allowances nobody pays for —
    # a provider quietly hitting its ceiling every day is the failure that costs
    # coverage without costing money, so it has to be visible daily.
    ai_html = ""
    providers = summary.get("providers") or []
    if providers:
        rows = []
        for p in providers:
            used, budget = p.get("used", 0), p.get("budget", 0)
            pct = (used * 100 // budget) if budget else 0
            # Amber past 60%, red past 90% — the point is to notice a ceiling
            # before it starts silently truncating a window.
            colour = "#DC2626" if pct >= 90 else ("#D97706" if pct >= 60 else "#16A34A")
            note = []
            if p.get("observed_limit"):
                note.append(f"észlelt plafon {p['observed_limit']}")
            if p.get("rate_limits"):
                note.append(f"{p['rate_limits']}× 429")
            if p.get("failures"):
                note.append(f"{p['failures']} hiba")
            rows.append(
                f"<tr><td style='padding:4px 12px 4px 0'>{p['name']}</td>"
                f"<td align='right' style='padding:4px 8px;color:{colour};font-weight:600'>"
                f"{used}</td>"
                f"<td align='right' style='padding:4px 8px;color:#8C8478'>{budget}</td>"
                f"<td align='right' style='padding:4px 8px'>{pct}%</td>"
                f"<td style='padding:4px 0 4px 8px;color:#8C8478;font-size:12px'>"
                f"{', '.join(note)}</td></tr>")
        ai_html = (
            "<h3 style='margin:18px 0 6px'>Ingyenes AI-keret (aznapi hívások)</h3>"
            "<table style='border-collapse:collapse;font-size:14px'>"
            "<tr style='color:#8C8478;font-size:12px'>"
            "<td style='padding:4px 12px 4px 0'>Szolgáltató</td>"
            "<td align='right' style='padding:4px 8px'>Hívás</td>"
            "<td align='right' style='padding:4px 8px'>Keret</td>"
            "<td align='right' style='padding:4px 8px'>%</td>"
            "<td style='padding:4px 0 4px 8px'></td></tr>"
            + "".join(rows) + "</table>")

        # The question this block could not answer: how much can we process in a
        # day, and are we collecting about that much? On 2026-08-18 we fetched
        # 2,434 pages and AI-processed 353 of them — a sevenfold over-collection
        # that took an afternoon of arithmetic to notice. Both numbers are
        # already here; the ratio is what makes them mean something.
        _calls = sum(int(p.get("used") or 0) for p in providers)
        _fails = sum(int(p.get("failures") or 0) for p in providers)
        _budget = sum(int(p.get("budget") or 0) for p in providers)
        _ok = max(0, _calls - _fails)
        _done = int(hu.get("pages_extracted") or 0) + int(intl.get("pages_extracted") or 0)
        _fetched = int(hu.get("pages_scraped") or 0) + int(intl.get("pages_scraped") or 0)
        if _done:
            per_page = _ok / _done
            capacity = int(_budget / per_page) if per_page else 0
            ratio = f"{_fetched / _done:.1f}×" if _done else "—"
            refused = f"{_fails * 100 // _calls}%" if _calls else "0%"
            ai_html += (
                "<p style='margin:8px 0 0;font-size:13px;color:#8C8478'>"
                f"{_ok} sikeres hívás / {_done} feldolgozott oldal = "
                f"<b>{per_page:.1f} hívás/oldal</b>. A teljes napi kerettel ez "
                f"<b>~{capacity:,} oldal/nap</b> kapacitás. "
                f"Letöltve {_fetched} oldal — {ratio} a feldolgozottnak. "
                f"Elutasított hívás: {refused}.</p>".replace(",", " "))

    runs_html = ""
    if summary["runs"]:
        items = []
        for r in summary["runs"]:
            # ⚠️ = the run finished, some items failed and are queued for the next
            # run; ❌ = it stopped early. One transient timeout out of 1414 pairs
            # is not the same event as a dead provider (2026-07-31).
            outcome = r.get("outcome") or ("ok" if r["success"] else "aborted")
            state = {"ok": "✅", "warning": "⚠️"}.get(outcome, "❌")
            fails = ""
            if r["search_failed"] or r["extract_failed"]:
                causes = [c for c in (r.get("search_error"), r.get("extract_error")) if c]
                cause = (f" · ok: {html_lib.escape(' / '.join(causes))}" if causes else "")
                colour = "#B4231F" if outcome == "aborted" else "#8A6410"
                fails = (f" — <span style='color:{colour}'>hibák: "
                         f"{r['search_failed']} keresés, {r['extract_failed']} oldal"
                         f" (nem cache-elve, a következő futás újrapróbálja){cause}</span>")
            if r.get("error"):
                fails += (f" — <span style='color:#B4231F'>futási hiba: "
                          f"{html_lib.escape(r['error'])}</span>")
            items.append(
                f"<li style='margin:3px 0'>{state} <b>{r['mode']}</b> · "
                f"{(r['started_at'] or '')[11:16]} UTC · {r['pairs']} város–téma páros · "
                f"{r['records']} rekord{fails}</li>")
        runs_html = ("<h3 style='margin:18px 0 6px'>Futások</h3>"
                     f"<ul style='margin:0;padding-left:18px'>{''.join(items)}</ul>")
    else:
        runs_html = "<p style='color:#8C8478'>Nem futott pipeline ezen a napon.</p>"

    html = f"""
<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;color:#1C1917;max-width:640px">
  <h2 style="margin:0 0 2px">Napi összefoglaló — {day}</h2>
  <p style="margin:0 0 16px;color:#8C8478;font-size:13px">közösségek.com + meetapedia.com</p>

  <h3 style="margin:0 0 6px">Látogatók{" (GA4)" if ga4 is not None else ""}</h3>
  <table style="border-collapse:collapse;font-size:14px">
    <tr style="color:#8C8478;font-size:12px">
      <td style="padding:4px 12px 4px 0"></td>
      <td align="right" style="padding:4px 8px">Látogató</td>
      {"<td align='right' style='padding:4px 8px'>Munkamenet</td>" if ga4 is not None else ""}
      <td align="right" style="padding:4px 0 4px 8px">Oldalletöltés</td></tr>
    {"".join(
        f"<tr><td style='padding:4px 12px 4px 0'>{site}.com</td>"
        f"<td align='right' style='padding:4px 8px;font-weight:600'>{g_site(site, 'visitors') if ga4 is not None else t_site(site, 'visitors')}</td>"
        + (f"<td align='right' style='padding:4px 8px'>{g_site(site, 'sessions')}</td>" if ga4 is not None else "")
        + f"<td align='right' style='padding:4px 0 4px 8px'>{g_site(site, 'pageviews') if ga4 is not None else t_site(site, 'pageviews')}</td></tr>"
        for site in ("kozossegek", "meetapedia"))}
    <tr style="border-top:1px solid #EAE5DB"><td style="padding:4px 12px 4px 0;font-weight:700">Összesen</td>
      <td align="right" style="padding:4px 8px;font-weight:700">{total_visitors}</td>
      {f"<td align='right' style='padding:4px 8px;font-weight:700'>{g_site('kozossegek', 'sessions') + g_site('meetapedia', 'sessions')}</td>" if ga4 is not None else ""}
      <td align="right" style="padding:4px 0 4px 8px;font-weight:700">{(g_site("kozossegek", "pageviews") + g_site("meetapedia", "pageviews")) if ga4 is not None else (t_site("kozossegek", "pageviews") + t_site("meetapedia", "pageviews"))}</td></tr>
  </table>
  <p style="color:#B5ADA0;font-size:11px;margin:4px 0 16px">
    {"Forrás: Google Analytics 4. Szerveroldali számláló (bot-szűrt): " + str(t_site("kozossegek", "visitors") + t_site("meetapedia", "visitors")) + " látogató." if ga4 is not None else "Szerveroldali számláló (botok kiszűrve); GA4-bekötéshez GA4_PROPERTY_ID + GA4_CREDENTIALS_JSON env kell."}</p>

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

  {ai_html}

  <h3 style="margin:18px 0 6px">Állomány (aktuális összesen)</h3>
  <table style="border-collapse:collapse;font-size:14px">
    <tr style="color:#8C8478;font-size:12px">
      <td style="padding:4px 12px 4px 0"></td>
      <td align="right" style="padding:4px 8px">Magyar</td>
      <td align="right" style="padding:4px 8px">Nemzetközi</td>
      <td align="right" style="padding:4px 0 4px 8px">Össz</td></tr>
    {stock_rows}
  </table>

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
    # Free-tier AI spend for the reported day. Best-effort: a report must still
    # go out if the router config is unreadable.
    try:
        from .providers import load_catalogue
        from .router import QuotaLedger
        cat = load_catalogue()
        ledger = QuotaLedger(db_path, day=day)
        summary["providers"] = [p for p in ledger.snapshot(cat)
                                if p["configured"] and p["used"]]
    except Exception as exc:
        log.warning("report_provider_usage_failed", error=str(exc))
        summary["providers"] = []
    traffic = get_traffic_for_day(db_path, day)
    ga4 = fetch_ga4_traffic(day)
    subject, html = build_report_html(day, summary, traffic, ga4)

    import resend
    resend.api_key = api_key
    resend.Emails.send({"from": sender, "to": [recipient],
                        "subject": subject, "html": html})
    log.info("daily_report_sent", day=day, to=recipient)
    return {"ok": True, "day": day, "to": recipient, "subject": subject}
