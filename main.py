#!/usr/bin/env python3
"""
Neft-qaz vakansiya botu

Axın:
  Apify (LinkedIn) -> Azərbaycan filtri -> köhnə ID-ləri at -> LLM təsnifatı
  -> Telegram -> data/jobs.json yenilə -> 30 gündən köhnələri sil

Lazım olan mühit dəyişənləri (GitHub Secrets):
  APIFY_TOKEN, OPENROUTER_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
İstəyə bağlı:
  WINDOW   -> "Past 24 hours" | "Past Week" | "Past Month" (boşdursa avtomatik)
  DRY_RUN  -> 1 olsa Telegram-a göndərmir, ekrana yazır
"""
import html
import json
import os
import re
import sys
import time
import datetime as dt
from concurrent.futures import ThreadPoolExecutor

import requests
import yaml

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT, "config.yaml")
STATE_PATH = os.path.join(ROOT, "data", "jobs.json")

APIFY_API = "https://api.apify.com/v2"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

BAKU_TZ = dt.timezone(dt.timedelta(hours=4))
DRY_RUN = os.environ.get("DRY_RUN") == "1"


def today() -> dt.date:
    return dt.datetime.now(BAKU_TZ).date()


def log(*a):
    print(*a, flush=True)


# ----------------------------------------------------------------------------
# Ayarlar və vəziyyət (state)
# ----------------------------------------------------------------------------
def load_config():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_state():
    if not os.path.exists(STATE_PATH):
        return {"jobs": {}, "seen": {}, "last_run": None}, True
    with open(STATE_PATH, encoding="utf-8") as f:
        st = json.load(f)
    st.setdefault("jobs", {})
    st.setdefault("seen", {})
    first_run = len(st["seen"]) == 0
    return st, first_run


def save_state(st, cfg):
    t = today()
    keep_jobs = dt.timedelta(days=cfg.get("retention_days", 30))
    keep_seen = dt.timedelta(days=cfg.get("seen_days", 90))
    st["jobs"] = {
        k: v for k, v in st["jobs"].items()
        if t - dt.date.fromisoformat(v["first_seen"]) <= keep_jobs
    }
    st["seen"] = {
        k: v for k, v in st["seen"].items()
        if t - dt.date.fromisoformat(v) <= keep_seen
    }
    st["last_run"] = dt.datetime.now(BAKU_TZ).isoformat(timespec="seconds")
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    # yeni elanlar üstdə olsun
    st["jobs"] = dict(sorted(st["jobs"].items(),
                             key=lambda kv: (kv[1]["first_seen"], kv[1].get("posted_date", "")),
                             reverse=True))
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=1)


# ----------------------------------------------------------------------------
# Apify
# ----------------------------------------------------------------------------
def apify_run(actor, run_input, token, timeout_s=1200, max_cost=1.0):
    """Aktoru işə salır, bitənə qədər gözləyir, nəticə siyahısını qaytarır."""
    headers = {"Authorization": f"Bearer {token}"}
    actor_id = actor.replace("/", "~")
    r = requests.post(
        f"{APIFY_API}/acts/{actor_id}/runs",
        params={"timeout": timeout_s, "maxTotalChargeUsd": max_cost},
        json=run_input, headers=headers, timeout=60,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Apify başlama xətası {r.status_code}: {r.text[:300]}")
    run = r.json()["data"]
    run_id = run["id"]

    deadline = time.time() + timeout_s + 180
    while True:
        time.sleep(8)
        s = requests.get(f"{APIFY_API}/actor-runs/{run_id}", headers=headers, timeout=60).json()["data"]
        status = s["status"]
        if status in ("SUCCEEDED", "TIMED-OUT"):
            if status == "TIMED-OUT":
                log("  ! Apify vaxtı bitdi, qismən nəticə götürülür")
            break
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Apify işləməsi {status}: {s.get('statusMessage', '')}")
        if time.time() > deadline:
            raise RuntimeError("Apify gözləmə vaxtı bitdi")

    ds = s["defaultDatasetId"]
    items, offset = [], 0
    while True:
        r = requests.get(
            f"{APIFY_API}/datasets/{ds}/items",
            params={"format": "json", "clean": "true", "offset": offset, "limit": 1000},
            headers=headers, timeout=120,
        )
        r.raise_for_status()
        batch = r.json()
        items += batch
        if len(batch) < 1000:
            break
        offset += 1000
    return items


def build_groups(cfg, window):
    base = {
        "early_applicant_only": False,
        "easy_apply": False,
        "include_contact_details": False,
        "include_full_company_details": False,
        "include_job_details": True,
        "location": cfg["location"],
        "posted_within": window,
    }
    groups = [("açar sözlər", {**base, "jobs_titles": cfg["role_keywords"],
                               "jobs_entries": cfg["jobs_entries_roles"]})]
    comps, n = cfg["companies"], cfg.get("company_chunk", 8)
    for i in range(0, len(comps), n):
        groups.append((f"şirkətlər {i // n + 1}", {
            **base, "company_names": comps[i:i + n],
            "jobs_entries": cfg["jobs_entries_companies"]}))
    return groups


AZ_RE = re.compile(
    r"azerbaijan|azərbaycan|азербайджан|baku|bakı|баку|sumqay|sumgait|absheron|abşeron|"
    r"gəncə|ganja|mingachevir|lankaran|shirvan|şirvan|neft daşlar|sangachal|səngəçal",
    re.I)


def norm(x):
    """Aktorun nəticəsini bizim sadə formata çevirir (bebity sahə adlarına da dözümlüdür)."""
    url = (x.get("job_url") or x.get("jobUrl") or x.get("apply_url") or "").split("?")[0]
    jid = str(x.get("job_id") or x.get("id") or "").strip()
    if not jid:
        m = re.search(r"(\d{8,})", url)
        jid = m.group(1) if m else ""
    if not url and jid:
        url = f"https://www.linkedin.com/jobs/view/{jid}"
    return {
        "id": jid,
        "title": (x.get("job_title") or x.get("title") or "").strip(),
        "company": (x.get("company_name") or x.get("companyName") or "").strip(),
        "location": (x.get("location") or "").strip(),
        "url": url,
        "posted_date": (x.get("posted_date") or x.get("publishedAt") or "")[:10],
        "description": x.get("job_description") or x.get("description") or "",
        "seniority": x.get("seniority_level") or x.get("experienceLevel") or "",
        "employment": x.get("employment_type") or x.get("contractType") or "",
        "industry": x.get("industries") or x.get("companyIndustries") or x.get("sector") or "",
    }


def scrape_all(cfg, window, token):
    """Bütün qrupları işlədir. Qrup xəta versə, digərləri davam edir."""
    groups = build_groups(cfg, window)
    found, errors = {}, []
    for name, run_input in groups:
        log(f"[Apify] {name}: başlayır ({window})")
        items = None
        for attempt in range(2):
            try:
                items = apify_run(cfg["actor"], run_input, token,
                                  max_cost=cfg.get("max_cost_per_run_usd", 1.0))
                break
            except Exception as e:  # noqa: BLE001
                log(f"  xəta: {e}")
                if attempt == 0 and "company_names" in run_input:
                    # bəzi aktor versiyaları şirkət + yer birlikdə qəbul etməyə bilər
                    run_input = {k: v for k, v in run_input.items() if k != "location"}
                    log("  yersiz təkrar cəhd (nəticə sonra Azərbaycan üzrə süzüləcək)")
                else:
                    time.sleep(5)
        if items is None:
            errors.append(name)
            continue
        kept = 0
        for raw in items:
            j = norm(raw)
            if not j["id"] or not j["title"]:
                continue
            if not AZ_RE.search(j["location"]):
                continue
            if j["id"] not in found:
                found[j["id"]] = j
                kept += 1
        log(f"  xam: {len(items)}, Azərbaycan və yeni (bu qrupda): {kept}")
    if errors and len(errors) == len(groups):
        raise RuntimeError("Bütün Apify qrupları xəta verdi")
    return found, errors


# ----------------------------------------------------------------------------
# LLM təsnifatı (OpenRouter)
# ----------------------------------------------------------------------------
SYSTEM_PROMPT = """You classify LinkedIn job postings for a petroleum-engineering student in Baku, Azerbaijan. He wants EVERY opportunity in the oil & gas sector (operators, service companies, EPC/engineering contractors, staffing firms serving oil & gas, state companies), especially internships, graduate / young-talent programmes and entry-level roles.

Return ONLY one JSON object, no markdown, no extra text:
{
 "relevant": "yes" | "maybe" | "no",
 "kind": "internship" | "graduate_programme" | "entry_level" | "experienced",
 "fit": "high" | "medium" | "low",
 "discipline": "petroleum" | "drilling" | "reservoir_geoscience" | "production_process" | "hse" | "subsea_marine" | "mechanical_piping" | "electrical_instrument" | "data_ml_it" | "project_commercial" | "admin_hr_finance" | "other",
 "nationals_only": true | false,
 "deadline": "short text or null",
 "summary_az": "1-2 short sentences in Azerbaijani: what the role is and the key requirements"
}

Rules:
- relevant = "yes" if the employer or the role clearly belongs to oil & gas / petroleum / energy services (any function, including HR or finance at an oil & gas company). "maybe" = unclear or adjacent energy (power, mining, petrochemicals, a general engineering contractor with no stated oil & gas link). "no" = banks, retail, games, general IT, consulting, hospitality etc. with no oil & gas link.
- Programmes named like "Discovery Week", "Young Talents", "Talent Programme", "Early Careers" at an oil & gas company are "graduate_programme".
- kind = "internship" for intern / trainee / placement / student programmes; "graduate_programme" for graduate schemes and young-talent programmes; "entry_level" for junior or no-experience roles; otherwise "experienced".
- fit = "high" for petroleum, drilling, reservoir, geoscience, production, process, HSE, data & ML roles in oil & gas; "medium" for other engineering/technical roles; "low" for admin, finance, HR, sales, support.
- nationals_only = true only if the posting says nationals/citizens only (e.g. "Nationals Only")."""

OIL_RE = re.compile(
    r"\b(oil|gas|petroleum|drilling|offshore|onshore|subsea|reservoir|refiner\w*|pipeline|upstream|"
    r"downstream|wellsite|lng|petrochem\w*|rig|socar|neft|qaz|qazma)\b", re.I)
INTERN_RE = re.compile(r"intern(ship)?\b|trainee|təcrübə|стаж", re.I)
GRAD_RE = re.compile(r"graduate|young talent|talent programme|talent program|early career|discovery week|apprentic", re.I)
ENTRY_RE = re.compile(r"junior|entry|assistant", re.I)


def llm_call(messages, cfg):
    key = os.environ["OPENROUTER_API_KEY"]
    body = {"model": cfg["llm_model"], "messages": messages,
            "temperature": 0, "max_tokens": 1500}
    last = None
    for attempt in range(4):
        try:
            r = requests.post(
                OPENROUTER_URL, json=body, timeout=90,
                headers={"Authorization": f"Bearer {key}",
                         "Content-Type": "application/json",
                         "X-Title": "oil-job-bot"})
            if r.status_code in (429, 500, 502, 503, 504):
                last = f"HTTP {r.status_code}"
                time.sleep(3 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"]
        except Exception as e:  # noqa: BLE001
            last = str(e)
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"LLM xətası: {last}")


def fallback_classify(j):
    """LLM işləməyəndə sadə qayda ilə təsnif edir (heç nə itməsin deyə)."""
    blob = f"{j['title']} {j['company']} {j['industry']}"
    rel = "maybe" if OIL_RE.search(blob) else "no"
    if INTERN_RE.search(j["title"]):
        kind = "internship"
    elif GRAD_RE.search(j["title"]):
        kind = "graduate_programme"
    elif ENTRY_RE.search(j["title"]) or j["seniority"] in ("Entry level", "Internship"):
        kind = "entry_level"
    else:
        kind = "experienced"
    return {"relevant": rel, "kind": kind, "fit": "medium", "discipline": "other",
            "nationals_only": bool(re.search(r"nationals? only", blob, re.I)),
            "deadline": None, "summary_az": "", "llm": False}


def classify(j, cfg):
    desc = (j["description"] or "")[: cfg.get("llm_description_chars", 2500)]
    payload = {
        "title": j["title"], "company": j["company"], "location": j["location"],
        "company_industry": j["industry"], "seniority": j["seniority"],
        "employment_type": j["employment"], "description": desc,
    }
    try:
        text = llm_call([
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ], cfg)
        m = re.search(r"\{.*\}", text, re.S)
        res = json.loads(m.group(0))
        res["relevant"] = res.get("relevant", "maybe") if res.get("relevant") in ("yes", "maybe", "no") else "maybe"
        if res.get("kind") not in ("internship", "graduate_programme", "entry_level", "experienced"):
            res["kind"] = "experienced"
        if res.get("fit") not in ("high", "medium", "low"):
            res["fit"] = "medium"
        res["llm"] = True
        return res
    except Exception as e:  # noqa: BLE001
        log(f"  LLM işləmədi ({j['id']}): {e} -> qayda ilə təsnif")
        return fallback_classify(j)


# ----------------------------------------------------------------------------
# Telegram
# ----------------------------------------------------------------------------
def tg_post(method, payload):
    if DRY_RUN:
        log(f"[DRY_RUN Telegram:{method}]", json.dumps(payload, ensure_ascii=False)[:600])
        return {"ok": True}
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    url = f"https://api.telegram.org/bot{token}/{method}"
    for _ in range(5):
        r = requests.post(url, json=payload, timeout=30)
        if r.status_code == 429:
            wait = r.json().get("parameters", {}).get("retry_after", 5)
            time.sleep(wait + 1)
            continue
        return r.json()
    return {"ok": False}


def send_text(text, button_url=None):
    payload = {"chat_id": os.environ.get("TELEGRAM_CHAT_ID", "0"), "text": text,
               "parse_mode": "HTML", "disable_web_page_preview": True}
    if button_url:
        payload["reply_markup"] = {"inline_keyboard": [[
            {"text": "🔗 LinkedIn-də aç", "url": button_url}]]}
    res = tg_post("sendMessage", payload)
    if not res.get("ok"):
        log("  Telegram xətası:", res)
    return bool(res.get("ok"))


KIND_LABEL = {
    "internship": "🎓 TƏCRÜBƏ (Intern)",
    "graduate_programme": "🎓 GƏNC / MƏZUN PROQRAMI",
    "entry_level": "🌱 Başlanğıc səviyyə",
    "experienced": "💼 Tam ştat",
}
DISC_LABEL = {
    "petroleum": "neft mühəndisliyi", "drilling": "qazma", "reservoir_geoscience": "layihə/geologiya",
    "production_process": "hasilat/proses", "hse": "HSE", "subsea_marine": "sualtı/dəniz",
    "mechanical_piping": "mexanika/boru", "electrical_instrument": "elektrik/avadanlıq",
    "data_ml_it": "data/ML/IT", "project_commercial": "layihə/kommersiya",
    "admin_hr_finance": "inzibati/HR/maliyyə", "other": "digər",
}
KIND_RANK = {"experienced": 0, "entry_level": 1, "graduate_programme": 2, "internship": 3}
FIT_RANK = {"low": 0, "medium": 1, "high": 2}


def format_message(j, c):
    e = html.escape
    lines = [f"<b>{e(KIND_LABEL.get(c['kind'], ''))}</b>"]
    if c["relevant"] == "maybe":
        lines[0] += "  ❓"
    lines.append(f"<b>{e(j['title'])}</b>")
    lines.append(f"🏢 {e(j['company'] or '—')}  ·  📍 {e(j['location'])}")
    meta = []
    if j["posted_date"]:
        meta.append(f"🗓 {j['posted_date']}")
    if c.get("discipline"):
        meta.append(DISC_LABEL.get(c["discipline"], ""))
    if c.get("nationals_only"):
        meta.append("🇦🇿 yalnız yerli vətəndaşlar")
    if meta:
        lines.append("  ·  ".join(x for x in meta if x))
    if c.get("deadline"):
        lines.append(f"⏳ Son tarix: {e(str(c['deadline']))}")
    if c.get("summary_az"):
        lines.append("")
        lines.append(e(c["summary_az"][:500]))
    return "\n".join(lines)[:3900]


# ----------------------------------------------------------------------------
# Əsas axın
# ----------------------------------------------------------------------------
def run():
    cfg = load_config()
    state, first_run = load_state()
    window = os.environ.get("WINDOW", "").strip() or (
        cfg["first_run_window"] if first_run else cfg["daily_window"])
    log(f"Başlanğıc: {today()}  |  ilk işləmə: {first_run}  |  tarix aralığı: {window}")

    if os.environ.get("MOCK_APIFY_FILE"):
        with open(os.environ["MOCK_APIFY_FILE"], encoding="utf-8") as f:
            raw = json.load(f)
        found = {}
        for x in raw:
            j = norm(x)
            if j["id"] and AZ_RE.search(j["location"]):
                found[j["id"]] = j
        errors = []
    else:
        found, errors = scrape_all(cfg, window, os.environ["APIFY_TOKEN"])

    new = [j for jid, j in found.items() if jid not in state["seen"]]
    log(f"Tapılan unikal Azərbaycan elanı: {len(found)}  |  yeni: {len(new)}")

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda j: classify(j, cfg), new))

    relevant, irrelevant = [], []
    for j, c in zip(new, results):
        (relevant if c["relevant"] in ("yes", "maybe") else irrelevant).append((j, c))
    log(f"Neft-qaza aid: {len(relevant)}  |  aid deyil: {len(irrelevant)}")

    t = today().isoformat()
    for j, _ in irrelevant:
        state["seen"][j["id"]] = t

    # Göndərmə sırası: əhəmiyyətsizlər əvvəl, intern/proqram ən sonda (söhbətdə ən aşağıda görünür)
    relevant.sort(key=lambda jc: (KIND_RANK[jc[1]["kind"]], FIT_RANK[jc[1]["fit"]], jc[0]["posted_date"]))

    if relevant and first_run:
        send_text(f"🚀 <b>İlk yükləmə</b>: son 30 günün {len(relevant)} neft-qaz elanı göndərilir. "
                  f"Ən vacibləri (təcrübə və gənc proqramları) ən sonda gələcək.")

    sent = 0
    for j, c in relevant:
        ok = send_text(format_message(j, c), j["url"])
        if ok:
            sent += 1
            state["seen"][j["id"]] = t
            state["jobs"][j["id"]] = {
                "id": j["id"], "title": j["title"], "company": j["company"],
                "location": j["location"], "url": j["url"], "posted_date": j["posted_date"],
                "first_seen": t, "kind": c["kind"], "fit": c["fit"],
                "discipline": c.get("discipline"), "relevant": c["relevant"],
                "summary_az": c.get("summary_az", ""),
            }
        time.sleep(1.2)  # Telegram limiti

    if cfg.get("daily_summary", True):
        interns = sum(1 for j, c in relevant if c["kind"] in ("internship", "graduate_programme"))
        msg = (f"✅ Yoxlama bitdi ({today().strftime('%d.%m.%Y')}): {sent} yeni neft-qaz elanı"
               + (f", onlardan {interns} təcrübə/proqram" if interns else "")
               + f". Baxılan yeni elan: {len(new)}.")
        if errors:
            msg += f"\n⚠️ {len(errors)} axtarış qrupu xəta verdi: {', '.join(errors)}"
        send_text(msg)

    save_state(state, cfg)
    log(f"Bitdi. Göndərildi: {sent}. Saxlanılan elan sayı: {len(state['jobs'])}")


def main():
    try:
        run()
    except Exception as e:  # noqa: BLE001
        log(f"XƏTA: {e}")
        try:
            send_text(f"⚠️ <b>Bot xətası</b>\n{html.escape(str(e))[:800]}")
        except Exception:  # noqa: BLE001
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
