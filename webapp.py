import os
import re
import json
import sqlite3
import subprocess
import tempfile
import threading
import time
from datetime import datetime, date
from typing import Optional
from urllib.parse import urlparse, urljoin

from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from scrapling.fetchers import Fetcher, DynamicFetcher, StealthyFetcher

app = FastAPI(title="Sports Scraper Hub")
templates = Jinja2Templates(directory="templates")

BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
DB_PATH       = os.path.join(BASE_DIR, "scraper_hub.db")


# ── database ──────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sites (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            name                TEXT    NOT NULL,
            url                 TEXT    NOT NULL UNIQUE,
            css_selector        TEXT    DEFAULT '',
            article_url_pattern TEXT    DEFAULT '',
            use_stealth         INTEGER DEFAULT 0,
            active              INTEGER DEFAULT 1,
            last_scraped        TEXT,
            created_at          TEXT    DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS articles (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id          INTEGER REFERENCES sites(id) ON DELETE CASCADE,
            title            TEXT    NOT NULL,
            article_url      TEXT    DEFAULT '',
            author           TEXT    DEFAULT '',
            category         TEXT    DEFAULT '',
            company_property TEXT    DEFAULT '',
            company_brand    TEXT    DEFAULT '',
            company_agency   TEXT    DEFAULT '',
            summary          TEXT    DEFAULT '',
            body             TEXT    DEFAULT '',
            scraped_at       TEXT    DEFAULT CURRENT_TIMESTAMP,
            scrape_date      TEXT    DEFAULT (date('now'))
        );
        CREATE TABLE IF NOT EXISTS email_logs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            subject     TEXT    DEFAULT '',
            recipients  TEXT    DEFAULT '',
            article_ids TEXT    DEFAULT '',
            body        TEXT    DEFAULT '',
            sent_at     TEXT    DEFAULT CURRENT_TIMESTAMP
        );
    """)
    for col, ddl in [
        ("article_url_pattern",   "ALTER TABLE sites ADD COLUMN article_url_pattern TEXT DEFAULT ''"),
        ("scrape_frequency_days", "ALTER TABLE sites ADD COLUMN scrape_frequency_days INTEGER DEFAULT 0"),
        ("auto_enrich",           "ALTER TABLE sites ADD COLUMN auto_enrich INTEGER DEFAULT 0"),
        ("body",                  "ALTER TABLE articles ADD COLUMN body TEXT DEFAULT ''"),
    ]:
        try:
            conn.execute(ddl)
        except Exception:
            pass

    if conn.execute("SELECT COUNT(*) FROM sites").fetchone()[0] == 0:
        conn.execute(
            "INSERT INTO sites (name, url, article_url_pattern) VALUES "
            "('2Playbook', 'https://www.2playbook.com/', '_102.html')"
        )
    conn.commit()
    conn.close()


init_db()


# ── scraping ──────────────────────────────────────────────────────────────────

def _page_signal_strength(page) -> int:
    """Return number of <a href> links — the strongest signal that we got real content."""
    if page is None:
        return 0
    try:
        return len(page.css("a[href]") or [])
    except Exception:
        return 0


def _fetch_page(url: str, use_stealth: bool):
    """Fetch a page with auto-escalation:
       Fetcher.get → DynamicFetcher.fetch → StealthyFetcher.fetch
       Escalates on HTTP errors (403/429/503), thin content, or no links."""
    if use_stealth:
        print(f"[scrape] forced stealthy-fetch {url}")
        try:
            return StealthyFetcher.fetch(url, headless=True)
        except Exception as e:
            print(f"[scrape] stealthy-fetch failed: {e}")
            return None

    best = None

    # 1) Plain HTTP
    try:
        print(f"[scrape] get {url}")
        page   = Fetcher.get(url, stealthy_headers=True, timeout=30)
        status = getattr(page, "status", 200)
        links  = _page_signal_strength(page)
        print(f"[scrape]   → status={status}, links={links}")
        if status < 400 and links >= 10:
            return page
        best = page if links > 0 else best
    except Exception as e:
        print(f"[scrape] get failed: {e}")

    # 2) Headless browser with JS
    try:
        print(f"[scrape] fetch (browser) {url}")
        page  = DynamicFetcher.fetch(url, disable_resources=True,
                                      network_idle=True, timeout=45000)
        links = _page_signal_strength(page)
        print(f"[scrape]   → links={links}")
        if links >= 10:
            return page
        if links > _page_signal_strength(best):
            best = page
    except Exception as e:
        print(f"[scrape] fetch failed: {e}")

    # 3) Stealth browser (anti-bot)
    try:
        print(f"[scrape] stealthy-fetch {url}")
        page  = StealthyFetcher.fetch(url, headless=True, network_idle=True)
        links = _page_signal_strength(page)
        print(f"[scrape]   → links={links}")
        if links > _page_signal_strength(best):
            best = page
    except Exception as e:
        print(f"[scrape] stealthy-fetch failed: {e}")

    return best


def fetch_site_links(url: str, css_selector: str, use_stealth: bool) -> list[dict]:
    """Return list of {title, url} for all <a> links on the page."""
    page = _fetch_page(url, use_stealth)
    if not page:
        return []

    # Narrow scope to CSS selector if given, then collect <a> inside it
    if css_selector:
        scope = page.css(css_selector) or []
        raw_links = []
        for el in scope:
            raw_links.extend(el.css("a[href]") or [])
        if not raw_links:                          # fallback: all links
            raw_links = page.css("a[href]") or []
    else:
        raw_links = page.css("a[href]") or []

    out = []
    for a in raw_links:
        href  = (a.attrib.get("href") or "").strip()
        title = " ".join((a.get_all_text() or "").split()).strip()
        if href and title:
            out.append({"url": urljoin(url, href), "title": title})
    return out


def fetch_article_body(url: str, use_stealth: bool = False) -> str:
    """Fetch and return the full text body of an article page."""
    page = _fetch_page(url, use_stealth)
    if not page:
        return ""
    # get_all_text returns clean visible text
    return (page.get_all_text() or "").strip()


def extract_articles(raw_links: list[dict], site) -> list[dict]:
    """Filter raw links to article candidates using the site's url_pattern or heuristics."""
    url_pattern = (site["article_url_pattern"] or "").strip()
    site_domain = urlparse(site["url"]).netloc

    SKIP = ("javascript:", "mailto:", ".pdf", ".xml",
            "facebook.", "twitter.", "linkedin.",
            "instagram.", "spotify.", "youtube.")

    seen, articles = set(), []
    for raw in raw_links:
        url   = raw["url"].strip()
        title = raw["title"].strip()

        if len(title) < 20 or url in seen:
            continue
        if any(x in url for x in SKIP):
            continue

        if url_pattern:
            if url_pattern not in url:
                continue
        else:
            # generic heuristic: same domain + at least 2 non-empty path segments
            parsed = urlparse(url)
            if parsed.netloc != site_domain:
                continue
            segs = [s for s in parsed.path.split("/") if s]
            if len(segs) < 2:
                continue

        parsed   = urlparse(url)
        segs     = [s for s in parsed.path.split("/") if s]
        category = segs[0] if segs else ""

        seen.add(url)
        articles.append(dict(
            title=title, article_url=url, category=category,
            author="", company_property="", company_brand="",
            company_agency="", summary="", body=""
        ))
    return articles[:60]


def enrich_with_ai(title: str, body: str, site_name: str = "") -> dict:
    """Use Gemini 2.5 Flash to extract Propiedad / Marca / Agencia / Resumen.
    Falls back to Anthropic Claude if GEMINI_API_KEY not set but ANTHROPIC_API_KEY is."""
    publisher_note = (
        f"\n- IMPORTANTE: El artículo se publica en «{site_name}». NUNCA incluyas «{site_name}» "
        f"en ningún campo, ni siquiera si aparece varias veces en el contenido (es el menú/header)."
        if site_name else
        "\n- NO incluyas el medio publicador (2Playbook, Palco23, Marca, El Mundo, etc.) en ningún campo."
    )
    prompt = (
        "Eres un analista de sports business. Analiza el artículo y extrae las entidades "
        "MENCIONADAS dentro del CUERPO de la noticia (no del menú/navegación). "
        "Devuelve SÓLO un JSON válido sin markdown.\n\n"
        "Reglas estrictas:" + publisher_note + "\n"
        "- Sólo cita entidades que aparezcan explícitamente como SUJETO de la noticia.\n"
        "- Si hay varias en un campo, sepáralas por coma. Si no hay ninguna, devuelve cadena vacía.\n\n"
        f"Título: {title}\n\nContenido:\n{body[:4000]}\n\n"
        'JSON requerido:\n{\n'
        '  "company_property": "propiedad deportiva (liga, club, federación, evento, competición)",\n'
        '  "company_brand":    "marca comercial (patrocinador, fabricante, anunciante, sponsor)",\n'
        '  "company_agency":   "agencia/intermediario/plataforma OTT/broadcaster (ej: IMG, DAZN, Mediapro, WPP). Cadena vacía si no hay ninguno mencionado.",\n'
        '  "summary":          "resumen ejecutivo de 1-2 frases en español, listo para correo profesional"\n}'
    )

    # Prefer Gemini (free tier — 2.5-flash-lite gives 1000 req/day vs 20 for flash)
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    if gemini_key:
        try:
            from google import genai
            from google.genai import types
            client = genai.Client(api_key=gemini_key)
            resp = client.models.generate_content(
                model="gemini-2.5-flash-lite",
                contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json"),
            )
            return json.loads(resp.text)
        except Exception as e:
            print(f"Gemini enrich error: {e}")

    # Fallback: Anthropic Claude
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if anthropic_key:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=anthropic_key)
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=400,
                messages=[{"role": "user", "content": prompt}]
            )
            return json.loads(msg.content[0].text)
        except Exception as e:
            print(f"Claude enrich error: {e}")

    return {}


def scrape_and_store(site_id: int) -> int:
    db   = get_db()
    site = db.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
    if not site:
        db.close(); return 0

    raw_links = fetch_site_links(site["url"], site["css_selector"] or "",
                                 bool(site["use_stealth"]))
    articles  = extract_articles(raw_links, site)

    today = date.today().isoformat()
    count = 0
    for a in articles:
        exists = db.execute(
            "SELECT id FROM articles WHERE site_id=? AND title=? AND scrape_date=?",
            (site_id, a["title"], today)
        ).fetchone()
        if not exists:
            db.execute("""
                INSERT INTO articles
                    (site_id,title,article_url,author,category,
                     company_property,company_brand,company_agency,summary,body,scrape_date)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (site_id, a["title"], a["article_url"], a["author"], a["category"],
                  a["company_property"], a["company_brand"], a["company_agency"],
                  a["summary"], a["body"], today))
            count += 1

    db.execute("UPDATE sites SET last_scraped=? WHERE id=?",
               (datetime.now().isoformat(), site_id))
    db.commit(); db.close()
    return count


# ── routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    db    = get_db()
    today = date.today().isoformat()
    stats = {
        "sites":  db.execute("SELECT COUNT(*) FROM sites WHERE active=1").fetchone()[0],
        "today":  db.execute("SELECT COUNT(*) FROM articles WHERE scrape_date=?", (today,)).fetchone()[0],
        "total":  db.execute("SELECT COUNT(*) FROM articles").fetchone()[0],
        "emails": db.execute("SELECT COUNT(*) FROM email_logs").fetchone()[0],
    }
    sites = db.execute("""
        SELECT s.*,
            (SELECT COUNT(*) FROM articles WHERE site_id=s.id AND scrape_date=?) AS today_count,
            (SELECT COUNT(*) FROM articles WHERE site_id=s.id) AS total_count
        FROM sites s WHERE s.active=1 ORDER BY s.last_scraped DESC
    """, (today,)).fetchall()
    recent = db.execute("""
        SELECT a.*, s.name AS site_name FROM articles a
        JOIN sites s ON s.id=a.site_id
        WHERE a.scrape_date=? ORDER BY a.scraped_at DESC LIMIT 8
    """, (today,)).fetchall()
    db.close()
    return templates.TemplateResponse(request=request, name="dashboard.html", context=dict(
        request=request, active_page="dashboard",
        stats=stats, sites=sites, recent=recent, today=today))


@app.get("/sites", response_class=HTMLResponse)
async def sites_list(request: Request):
    db    = get_db()
    today = date.today().isoformat()
    sites = db.execute("""
        SELECT s.*,
            (SELECT COUNT(*) FROM articles WHERE site_id=s.id AND scrape_date=?) AS today_count,
            (SELECT COUNT(*) FROM articles WHERE site_id=s.id) AS total_count
        FROM sites s ORDER BY s.created_at DESC
    """, (today,)).fetchall()
    db.close()
    return templates.TemplateResponse(request=request, name="sites.html", context=dict(
        request=request, active_page="sites", sites=sites))


@app.post("/sites")
async def add_site(name: str = Form(...), url: str = Form(...),
                   css_selector: str = Form(""), use_stealth: int = Form(0),
                   article_url_pattern: str = Form("")):
    db = get_db()
    try:
        db.execute(
            "INSERT INTO sites (name,url,css_selector,use_stealth,article_url_pattern) VALUES (?,?,?,?,?)",
            (name, url.strip(), css_selector.strip(), use_stealth, article_url_pattern.strip())
        )
        db.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        db.close()
    return RedirectResponse("/sites", status_code=303)


def resolve_media_url(name: str) -> dict:
    """Use Gemini to resolve a media outlet name → homepage URL."""
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return {"name": name, "url": "", "found": False, "reason": "no API key"}
    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=api_key)
        prompt = (
            "Eres experto en medios de comunicación. Para el siguiente medio, "
            "devuelve SÓLO un JSON válido sin markdown.\n\n"
            f"Medio: {name}\n\n"
            "JSON requerido:\n"
            "{\n"
            '  "official_name": "nombre oficial del medio",\n'
            '  "url": "https://www.dominio.com/" (homepage exacta, con https y trailing slash),\n'
            '  "found": true si conoces el medio con certeza, false si no estás seguro\n'
            "}"
        )
        resp = client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json"),
        )
        data = json.loads(resp.text)
        return {
            "name":  data.get("official_name") or name,
            "url":   data.get("url", ""),
            "found": bool(data.get("found")),
        }
    except Exception as e:
        return {"name": name, "url": "", "found": False, "reason": str(e)}


def parse_media_list(text: str) -> list[dict]:
    """Parse a text/CSV blob into a list of {name, url} dicts.
    Each line can be:
      - Just a name:      "Marca"
      - Name + URL (CSV): "Marca, https://www.marca.com/"
    """
    out = []
    seen = set()
    for line in text.splitlines():
        line = line.strip().strip('"')
        if not line or line.startswith("#"):
            continue
        # Skip header row
        if line.lower().startswith(("name,", "nombre,", "medio,")):
            continue
        parts = [p.strip().strip('"') for p in line.split(",")]
        name = parts[0]
        url  = parts[1] if len(parts) > 1 and parts[1].startswith("http") else ""
        if name and name.lower() not in seen:
            seen.add(name.lower())
            out.append({"name": name, "url": url})
    return out


@app.post("/sites/import/preview")
async def import_preview(file: Optional[UploadFile] = File(None),
                         text: str = Form("")):
    """Parse uploaded file or text, resolve missing URLs via Gemini.
    Returns a list of {name, url, found} for the user to review."""
    blob = ""
    if file is not None and file.filename:
        try:
            blob = (await file.read()).decode("utf-8", errors="replace")
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    if text:
        blob = (blob + "\n" + text) if blob else text

    if not blob.strip():
        return JSONResponse({"ok": False, "error": "empty input"}, status_code=400)

    items = parse_media_list(blob)

    # Already-existing URLs: mark them so the user knows
    db = get_db()
    existing_names = {r["name"].lower() for r in db.execute("SELECT name FROM sites").fetchall()}
    existing_urls  = {r["url"]          for r in db.execute("SELECT url  FROM sites").fetchall()}
    db.close()

    results = []
    for item in items:
        name = item["name"]
        url  = item["url"]
        if not url:
            resolved = resolve_media_url(name)
            url      = resolved["url"]
            found    = resolved["found"]
            official = resolved["name"]
        else:
            found    = True
            official = name
        results.append({
            "name":     official,
            "url":      url,
            "found":    found,
            "duplicate": (official.lower() in existing_names) or (url in existing_urls),
        })
    return JSONResponse({"ok": True, "items": results,
                         "ai_used": bool(os.environ.get("GEMINI_API_KEY"))})


@app.post("/sites/import/confirm")
async def import_confirm(request: Request):
    """Accept JSON list of sites and add them all."""
    payload = await request.json()
    items   = payload.get("items", [])
    db      = get_db()
    added   = 0
    skipped = 0
    for it in items:
        name = (it.get("name") or "").strip()
        url  = (it.get("url")  or "").strip()
        if not name or not url:
            skipped += 1; continue
        try:
            db.execute("INSERT INTO sites (name,url,article_url_pattern) VALUES (?,?,?)",
                       (name, url, (it.get("article_url_pattern") or "").strip()))
            added += 1
        except sqlite3.IntegrityError:
            skipped += 1
    db.commit(); db.close()
    return JSONResponse({"ok": True, "added": added, "skipped": skipped})


@app.post("/sites/{site_id}/edit")
async def edit_site(site_id: int,
                    name: str = Form(...),
                    url: str = Form(...),
                    css_selector: str = Form(""),
                    use_stealth: int = Form(0),
                    article_url_pattern: str = Form("")):
    db = get_db()
    try:
        db.execute("""UPDATE sites
                      SET name=?, url=?, css_selector=?, use_stealth=?, article_url_pattern=?
                      WHERE id=?""",
                   (name, url.strip(), css_selector.strip(), use_stealth,
                    article_url_pattern.strip(), site_id))
        db.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        db.close()
    return RedirectResponse("/sites", status_code=303)


@app.post("/sites/{site_id}/delete")
async def delete_site(site_id: int):
    db = get_db()
    db.execute("DELETE FROM sites WHERE id=?", (site_id,))
    db.commit(); db.close()
    return RedirectResponse("/sites", status_code=303)


@app.post("/sites/{site_id}/toggle")
async def toggle_site(site_id: int):
    db = get_db()
    db.execute("UPDATE sites SET active=1-active WHERE id=?", (site_id,))
    db.commit(); db.close()
    return RedirectResponse("/sites", status_code=303)


@app.post("/sites/{site_id}/scrape")
def scrape_site_now(site_id: int):
    """Sync def → FastAPI runs in threadpool so Playwright sync API works."""
    scrape_and_store(site_id)
    return RedirectResponse("/news", status_code=303)


@app.post("/scrape-all")
def scrape_all():
    """Sync def → FastAPI runs in threadpool so Playwright sync API works."""
    db    = get_db()
    sites = db.execute("SELECT id FROM sites WHERE active=1").fetchall()
    db.close()
    for s in sites:
        scrape_and_store(s["id"])
    return RedirectResponse("/news", status_code=303)


@app.get("/news", response_class=HTMLResponse)
async def news(request: Request, date_filter: Optional[str] = None,
               site_id: Optional[str] = None, q: Optional[str] = None):
    db     = get_db()
    where  = []
    params = []

    # Accept site_id as string so empty form values don't 422
    site_id_int = int(site_id) if site_id and site_id.isdigit() else None

    if q:
        # When searching, scan title + body + summary + entities across all dates
        where.append("""(
            a.title LIKE ? OR a.body LIKE ? OR a.summary LIKE ?
            OR a.company_property LIKE ? OR a.company_brand LIKE ?
            OR a.company_agency LIKE ?
        )""")
        like = f"%{q}%"
        params.extend([like, like, like, like, like, like])
        filter_date = date_filter or ""   # show whatever dates match
    else:
        filter_date = date_filter or date.today().isoformat()
        where.append("a.scrape_date=?")
        params.append(filter_date)

    if site_id_int:
        where.append("a.site_id=?")
        params.append(site_id_int)

    where_sql = " AND ".join(where) if where else "1=1"
    articles = db.execute(f"""
        SELECT a.*, s.name AS site_name FROM articles a
        JOIN sites s ON s.id=a.site_id
        WHERE {where_sql}
        ORDER BY a.scrape_date DESC, a.id DESC
        LIMIT 300
    """, params).fetchall()

    sites = db.execute("SELECT id,name FROM sites WHERE active=1 ORDER BY name").fetchall()
    dates = [r["scrape_date"] for r in
             db.execute("SELECT DISTINCT scrape_date FROM articles ORDER BY scrape_date DESC LIMIT 30").fetchall()]
    db.close()
    return templates.TemplateResponse(request=request, name="news.html", context=dict(
        request=request, active_page="news", articles=articles,
        sites=sites, filter_date=filter_date, filter_site=site_id_int,
        search_q=q or "", available_dates=dates))


@app.post("/articles/{article_id}")
async def update_article(article_id: int,
                         company_property: str = Form(""),
                         company_brand:    str = Form(""),
                         company_agency:   str = Form(""),
                         summary:          str = Form("")):
    db = get_db()
    db.execute("""
        UPDATE articles SET company_property=?,company_brand=?,company_agency=?,summary=?
        WHERE id=?
    """, (company_property, company_brand, company_agency, summary, article_id))
    db.commit(); db.close()
    return JSONResponse({"ok": True})


def _enrich_article(article_id: int) -> dict:
    """Reusable enrichment logic shared by HTTP endpoint and scheduler."""
    db  = get_db()
    art = db.execute(
        "SELECT a.*, s.use_stealth, s.name AS site_name FROM articles a "
        "JOIN sites s ON s.id=a.site_id WHERE a.id=?",
        (article_id,)
    ).fetchone()
    if not art:
        db.close()
        return {"ok": False, "error": "not found"}

    body = fetch_article_body(art["article_url"], bool(art["use_stealth"]))
    ai   = enrich_with_ai(art["title"], body, art["site_name"])

    publishers = [r["name"] for r in db.execute("SELECT name FROM sites").fetchall()]
    def _strip_publishers(value: str) -> str:
        if not value:
            return ""
        items = [x.strip() for x in value.split(",")]
        items = [x for x in items if x and not any(p.lower() in x.lower() for p in publishers)]
        return ", ".join(items)

    if ai:
        new_prop   = _strip_publishers(ai.get("company_property", ""))
        new_brand  = _strip_publishers(ai.get("company_brand", ""))
        new_agency = _strip_publishers(ai.get("company_agency", ""))
        new_sum    = ai.get("summary", "") or ""
    else:
        new_prop   = art["company_property"] or ""
        new_brand  = art["company_brand"]    or ""
        new_agency = art["company_agency"]   or ""
        new_sum    = art["summary"]          or ""

    db.execute("""
        UPDATE articles SET body=?, company_property=?, company_brand=?,
            company_agency=?, summary=? WHERE id=?
    """, (body, new_prop, new_brand, new_agency, new_sum, article_id))
    db.commit(); db.close()

    return {
        "ok":               True,
        "company_property": new_prop,
        "company_brand":    new_brand,
        "company_agency":   new_agency,
        "summary":          new_sum,
        "has_body":         bool(body),
        "body_preview":     body[:400] if body else "",
        "ai_used":          bool(os.environ.get("GEMINI_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")),
    }


@app.get("/articles/{article_id}/body")
def get_article_body(article_id: int):
    """Return full body text of an article."""
    db = get_db()
    art = db.execute(
        "SELECT id, title, article_url, body FROM articles WHERE id=?",
        (article_id,)
    ).fetchone()
    db.close()
    if not art:
        return JSONResponse({"ok": False}, status_code=404)
    return JSONResponse({
        "ok":    True,
        "id":    art["id"],
        "title": art["title"],
        "url":   art["article_url"],
        "body":  art["body"] or "",
    })


@app.post("/articles/{article_id}/enrich")
def enrich_article(article_id: int):
    """Scrape article body and run AI extraction. Sync def → runs in threadpool."""
    result = _enrich_article(article_id)
    if not result.get("ok"):
        return JSONResponse(result, status_code=404)
    return JSONResponse(result)


def auto_enrich_site(site_id: int, max_articles: int = 80):
    """Enrich any articles for this site that don't have a body yet."""
    db = get_db()
    arts = db.execute("""
        SELECT id FROM articles
        WHERE site_id=? AND (body='' OR body IS NULL)
        ORDER BY id DESC LIMIT ?
    """, (site_id, max_articles)).fetchall()
    db.close()
    print(f"[auto-enrich] site={site_id}, {len(arts)} articles to enrich")
    for a in arts:
        try:
            _enrich_article(a["id"])
            time.sleep(1)
        except Exception as e:
            print(f"[auto-enrich] failed for article {a['id']}: {e}")


@app.get("/aggregated", response_class=HTMLResponse)
async def aggregated(request: Request, site_id: Optional[str] = None,
                     category: Optional[str] = None, q: Optional[str] = None,
                     page: int = 1):
    db     = get_db()
    where  = ["1=1"]
    params = []

    site_id_int = int(site_id) if site_id and site_id.isdigit() else None

    if site_id_int:
        where.append("a.site_id=?"); params.append(site_id_int)
    if category:
        where.append("a.category=?"); params.append(category)
    if q:
        where.append("(a.title LIKE ? OR a.summary LIKE ? OR a.company_brand LIKE ? OR a.body LIKE ?)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%"]
    base   = f"FROM articles a JOIN sites s ON s.id=a.site_id WHERE {' AND '.join(where)}"
    total  = db.execute(f"SELECT COUNT(*) {base}", params).fetchone()[0]
    per_pg = 50
    offset = (page - 1) * per_pg
    rows   = db.execute(
        f"SELECT a.*,s.name AS site_name {base} ORDER BY a.scrape_date DESC,a.id DESC "
        f"LIMIT {per_pg} OFFSET {offset}", params
    ).fetchall()
    sites  = db.execute("SELECT id,name FROM sites ORDER BY name").fetchall()
    cats   = [r["category"] for r in
              db.execute("SELECT DISTINCT category FROM articles WHERE category!='' ORDER BY category").fetchall()]
    db.close()
    return templates.TemplateResponse(request=request, name="aggregated.html", context=dict(
        request=request, active_page="aggregated", articles=rows,
        sites=sites, categories=cats, filter_site=site_id_int,
        filter_category=category, search_q=q or "",
        total=total, page=page, per_page=per_pg,
        total_pages=max(1, (total + per_pg - 1) // per_pg)))


@app.get("/email", response_class=HTMLResponse)
async def email_page(request: Request, article_ids: Optional[str] = None):
    db       = get_db()
    selected = []
    if article_ids:
        ids = [int(i) for i in article_ids.split(",") if i.strip().isdigit()]
        if ids:
            ph       = ",".join("?" * len(ids))
            selected = db.execute(f"""
                SELECT a.*,s.name AS site_name FROM articles a
                JOIN sites s ON s.id=a.site_id WHERE a.id IN ({ph})
            """, ids).fetchall()
    today_arts = db.execute("""
        SELECT a.*,s.name AS site_name FROM articles a
        JOIN sites s ON s.id=a.site_id
        WHERE a.scrape_date=date('now') ORDER BY a.site_id, a.id
    """).fetchall()
    logs = db.execute("SELECT * FROM email_logs ORDER BY sent_at DESC LIMIT 10").fetchall()
    db.close()
    return templates.TemplateResponse(request=request, name="email.html", context=dict(
        request=request, active_page="email",
        selected_articles=selected, today_articles=today_arts, email_logs=logs))


@app.post("/email/log")
async def log_email(subject: str = Form(""), recipients: str = Form(""),
                    article_ids: str = Form(""), body: str = Form("")):
    db = get_db()
    db.execute("INSERT INTO email_logs (subject,recipients,article_ids,body) VALUES (?,?,?,?)",
               (subject, recipients, article_ids, body))
    db.commit(); db.close()
    return JSONResponse({"ok": True})


# ── settings & scheduler ──────────────────────────────────────────────────────

@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    db = get_db()
    sites = db.execute("""
        SELECT s.*,
            (SELECT COUNT(*) FROM articles WHERE site_id=s.id) AS total_count,
            (SELECT COUNT(*) FROM articles WHERE site_id=s.id AND scrape_date=date('now')) AS today_count
        FROM sites s ORDER BY s.name
    """).fetchall()
    db.close()
    return templates.TemplateResponse(request=request, name="settings.html", context=dict(
        request=request, active_page="settings", sites=sites,
        ai_configured=bool(os.environ.get("GEMINI_API_KEY") or os.environ.get("ANTHROPIC_API_KEY"))))


@app.post("/sites/{site_id}/schedule")
def update_schedule(site_id: int,
                    scrape_frequency_days: int = Form(0),
                    auto_enrich:           int = Form(0)):
    db = get_db()
    db.execute("UPDATE sites SET scrape_frequency_days=?, auto_enrich=? WHERE id=?",
               (scrape_frequency_days, auto_enrich, site_id))
    db.commit()
    row = db.execute("SELECT scrape_frequency_days, auto_enrich, last_scraped FROM sites WHERE id=?",
                     (site_id,)).fetchone()
    db.close()
    return JSONResponse({
        "ok":                    True,
        "scrape_frequency_days": row["scrape_frequency_days"],
        "auto_enrich":           bool(row["auto_enrich"]),
        "last_scraped":          row["last_scraped"],
    })


def scheduler_loop():
    """Background thread: every 5 min, scrape sites that are due."""
    print("[scheduler] started, checking every 5 min")
    while True:
        try:
            db = get_db()
            due = db.execute("""
                SELECT * FROM sites
                WHERE active=1 AND scrape_frequency_days > 0
                  AND (last_scraped IS NULL OR
                       julianday('now') - julianday(last_scraped) >= scrape_frequency_days)
            """).fetchall()
            db.close()
            for site in due:
                print(f"[scheduler] scraping '{site['name']}' (every {site['scrape_frequency_days']} day(s))")
                try:
                    new_count = scrape_and_store(site["id"])
                    print(f"[scheduler]   → {new_count} new articles")
                    if site["auto_enrich"] and new_count > 0:
                        print(f"[scheduler]   auto-enriching '{site['name']}'...")
                        auto_enrich_site(site["id"])
                except Exception as e:
                    print(f"[scheduler]   error: {e}")
        except Exception as e:
            print(f"[scheduler] loop error: {e}")
        time.sleep(300)  # 5 min


# Start the scheduler thread (daemon so it dies with the process)
threading.Thread(target=scheduler_loop, daemon=True, name="scheduler").start()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("webapp:app", host="0.0.0.0", port=8000, reload=True)
