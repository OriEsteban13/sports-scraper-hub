"""
One-time migration: SQLite (scraper_hub.db) → Supabase PostgreSQL.
Usage:
    DATABASE_URL="postgresql://..." python migrate_to_supabase.py
"""
import os, sys, sqlite3, psycopg2, psycopg2.extras

SRC = os.path.join(os.path.dirname(__file__), "scraper_hub.db")
DST = os.environ.get("DATABASE_URL", "")

if not DST:
    print("ERROR: set DATABASE_URL env var first.")
    sys.exit(1)

# Add connect_timeout and keepalives to avoid SSL timeout
DST_PARAMS = DST + ("&" if "?" in DST else "?") + "connect_timeout=30&keepalives=1&keepalives_idle=30"

print(f"\nConectando a Supabase...")
src = sqlite3.connect(SRC)
src.row_factory = sqlite3.Row
dst = psycopg2.connect(DST_PARAMS)
dst.autocommit = False
cur = dst.cursor()
print("Conexión OK\n")

# ── Schema creation (DDL only, no webapp import) ────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS sites (
    id           SERIAL PRIMARY KEY,
    name         TEXT    NOT NULL,
    url          TEXT    NOT NULL UNIQUE,
    css_selector TEXT    DEFAULT '',
    use_stealth  INTEGER DEFAULT 0,
    active       INTEGER DEFAULT 1,
    last_scraped TEXT,
    created_at   TEXT    DEFAULT CURRENT_TIMESTAMP,
    article_url_pattern TEXT DEFAULT '',
    scrape_frequency_days INTEGER DEFAULT 0,
    auto_enrich INTEGER DEFAULT 0,
    last_scrape_status TEXT DEFAULT '',
    last_scrape_error TEXT DEFAULT '',
    last_scrape_count INTEGER DEFAULT 0,
    last_traffic_check TEXT,
    monthly_visits_manual INTEGER DEFAULT 0,
    traffic_frequency_days INTEGER DEFAULT 5,
    category TEXT DEFAULT '',
    country  TEXT DEFAULT 'WW',
    cookies_json TEXT DEFAULT '',
    cdp_url TEXT DEFAULT '',
    last_working_tier INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS site_traffic (
    id           SERIAL PRIMARY KEY,
    site_id      INTEGER REFERENCES sites(id) ON DELETE CASCADE,
    measured_at  TEXT    NOT NULL,
    monthly_visits BIGINT DEFAULT 0,
    source       TEXT    DEFAULT '',
    bounce_rate  REAL    DEFAULT 0,
    pages_per_visit REAL DEFAULT 0,
    global_rank  INTEGER DEFAULT 0,
    country_rank INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS articles (
    id           SERIAL PRIMARY KEY,
    site_id      INTEGER REFERENCES sites(id) ON DELETE CASCADE,
    title        TEXT    NOT NULL,
    article_url  TEXT    DEFAULT '',
    author       TEXT    DEFAULT '',
    category     TEXT    DEFAULT '',
    company_property TEXT DEFAULT '',
    company_brand    TEXT DEFAULT '',
    company_agency   TEXT DEFAULT '',
    summary      TEXT    DEFAULT '',
    body         TEXT    DEFAULT '',
    scrape_date  TEXT    NOT NULL,
    images       TEXT    DEFAULT '',
    videos       TEXT    DEFAULT '',
    enriched_at  TEXT,
    sentiment    TEXT    DEFAULT '',
    tier_used    INTEGER DEFAULT 1,
    media_local  TEXT    DEFAULT '',
    media_remote TEXT    DEFAULT ''
);

CREATE TABLE IF NOT EXISTS saved_searches (
    id           SERIAL PRIMARY KEY,
    name         TEXT    NOT NULL,
    query        TEXT    NOT NULL,
    created_at   TEXT    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS email_logs (
    id           SERIAL PRIMARY KEY,
    recipient    TEXT    NOT NULL,
    subject      TEXT    NOT NULL,
    body_html    TEXT    DEFAULT '',
    sent_at      TEXT    DEFAULT CURRENT_TIMESTAMP,
    status       TEXT    DEFAULT 'sent'
);

CREATE TABLE IF NOT EXISTS clients (
    id           SERIAL PRIMARY KEY,
    name         TEXT    NOT NULL,
    notes        TEXT    DEFAULT '',
    created_at   TEXT    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS client_searches (
    id           SERIAL PRIMARY KEY,
    client_id    INTEGER REFERENCES clients(id) ON DELETE CASCADE,
    name         TEXT    NOT NULL,
    query        TEXT    NOT NULL,
    created_at   TEXT    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS client_brands (
    id           SERIAL PRIMARY KEY,
    client_id    INTEGER REFERENCES clients(id) ON DELETE CASCADE,
    query        TEXT    NOT NULL,
    created_at   TEXT    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS scrape_queue (
    id           SERIAL PRIMARY KEY,
    site_id      INTEGER REFERENCES sites(id) ON DELETE CASCADE,
    queued_at    TEXT    DEFAULT CURRENT_TIMESTAMP,
    status       TEXT    DEFAULT 'pending'
);
"""

print("Creando tablas...")
for stmt in SCHEMA.split(";"):
    stmt = stmt.strip()
    if stmt:
        try:
            cur.execute(stmt)
        except Exception as e:
            dst.rollback()
            print(f"  DDL error (ignorando): {e}")
dst.commit()
print("Tablas OK\n")

# ── Data migration ──────────────────────────────────────────────────────────

def rows(table):
    return src.execute(f"SELECT * FROM {table}").fetchall()

def cols(table):
    return [d[0] for d in src.execute(f"SELECT * FROM {table} LIMIT 0").description]

def get_conn():
    """Fresh connection with keepalive settings."""
    return psycopg2.connect(DST_PARAMS)

def insert_batch(sql, batch, retries=5):
    """Try to insert a batch, reconnecting up to `retries` times."""
    import time
    for attempt in range(retries):
        try:
            conn = get_conn()
            c2   = conn.cursor()
            c2.executemany(sql, batch)
            conn.commit()
            conn.close()
            return len(batch), 0
        except Exception as e:
            try: conn.close()
            except: pass
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # 1s, 2s, 4s, 8s...
            else:
                # Last attempt: insert one by one
                ok2 = skip2 = 0
                for r in batch:
                    for _ in range(3):
                        try:
                            conn2 = get_conn()
                            c3    = conn2.cursor()
                            c3.execute(sql, r)
                            conn2.commit()
                            conn2.close()
                            ok2 += 1
                            break
                        except Exception:
                            try: conn2.close()
                            except: pass
                            time.sleep(1)
                    else:
                        skip2 += 1
                return ok2, skip2
    return 0, len(batch)

def insert_all(table, conflict="id"):
    import time
    data = rows(table)
    if not data:
        print(f"  {table}: 0 filas")
        return
    c = cols(table)
    placeholders = ", ".join(["%s"] * len(c))
    col_list     = ", ".join(c)
    sql = (f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
           f"ON CONFLICT ({conflict}) DO NOTHING")

    # Find max already-migrated id to resume
    try:
        conn = get_conn()
        cur2 = conn.cursor()
        cur2.execute(f"SELECT COALESCE(MAX(id),0) FROM {table}")
        max_done = cur2.fetchone()[0]
        conn.close()
    except Exception:
        max_done = 0

    remaining = [r for r in data if r["id"] > max_done]
    total = len(data)
    already = total - len(remaining)
    if already:
        print(f"  {table}: {already} ya migrados, continuando desde id>{max_done}")

    ok = already
    skip = 0
    BATCH = 20

    for i in range(0, len(remaining), BATCH):
        batch = [tuple(r) for r in remaining[i:i+BATCH]]
        b_ok, b_skip = insert_batch(sql, batch)
        ok   += b_ok
        skip += b_skip
        if i % 200 == 0:
            print(f"    {table}: {ok}/{total}...", flush=True)
        time.sleep(0.1)  # brief pause to avoid rate limits

    print(f"  {table}: {ok} insertados, {skip} omitidos")

src_sites = src.execute("SELECT COUNT(*) FROM sites").fetchone()[0]
src_arts  = src.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
print(f"Origen: {src_sites} sitios, {src_arts} artículos\n")

print("Migrando datos...")
insert_all("sites",           conflict="id")
insert_all("site_traffic",    conflict="id")
insert_all("articles",        conflict="id")
insert_all("saved_searches",  conflict="id")
insert_all("clients",         conflict="id")
insert_all("client_searches", conflict="id")
insert_all("client_brands",   conflict="id")

# Reset sequences so new inserts get correct IDs
print("\nActualizando secuencias...")
conn_seq = get_conn()
cur_seq  = conn_seq.cursor()
for table in ["sites","site_traffic","articles","saved_searches",
              "clients","client_searches","client_brands"]:
    cur_seq.execute(f"SELECT setval(pg_get_serial_sequence('{table}','id'), COALESCE(MAX(id),1)) FROM {table}")
conn_seq.commit()
conn_seq.close()

src.close()
print("\n✓ Migración completada. Verifica en el dashboard de Supabase.")
