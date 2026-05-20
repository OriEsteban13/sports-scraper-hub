"""
One-time migration: SQLite (scraper_hub.db) → Supabase PostgreSQL.

Usage:
    DATABASE_URL="postgresql://..." python migrate_to_supabase.py

The script:
  1. Connects to Supabase PostgreSQL via DATABASE_URL
  2. Runs init_db() to create all tables
  3. Copies all rows from every table in the correct FK order
  4. Is safe to re-run (uses ON CONFLICT DO NOTHING for most tables)
"""

import os, sys, sqlite3, psycopg2, json
from datetime import datetime

SRC = os.path.join(os.path.dirname(__file__), "scraper_hub.db")
DST = os.environ.get("DATABASE_URL", "")

if not DST:
    print("ERROR: set DATABASE_URL env var first.")
    sys.exit(1)

src = sqlite3.connect(SRC)
src.row_factory = sqlite3.Row
dst = psycopg2.connect(DST)
dst.autocommit = False
cur = dst.cursor()

# ── helpers ────────────────────────────────────────────────────────────────────

def rows(table):
    return src.execute(f"SELECT * FROM {table}").fetchall()

def cols(table):
    return [d[0] for d in src.execute(f"SELECT * FROM {table} LIMIT 0").description]

def insert_all(table, conflict="id"):
    data = rows(table)
    if not data:
        print(f"  {table}: 0 rows (empty)")
        return
    c = cols(table)
    placeholders = ", ".join(["%s"] * len(c))
    col_list     = ", ".join(c)
    sql = (f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
           f"ON CONFLICT ({conflict}) DO NOTHING")
    ok = skip = 0
    for row in data:
        try:
            cur.execute(sql, tuple(row))
            ok += 1
        except Exception as e:
            skip += 1
            print(f"    skip row {dict(row)}: {e}")
            dst.rollback()
    dst.commit()
    print(f"  {table}: {ok} inserted, {skip} skipped")

# ── run ────────────────────────────────────────────────────────────────────────

print(f"\nMigrating {SRC} → Supabase PostgreSQL")
print(f"Source: {src.execute('SELECT COUNT(*) FROM sites').fetchone()[0]} sites")
print()

# Import webapp to trigger init_db() (creates all tables on Supabase)
sys.path.insert(0, os.path.dirname(__file__))
import webapp
webapp.init_db()
print("Tables created / verified.\n")

# Insert in FK-safe order
insert_all("sites",          conflict="id")
insert_all("site_traffic",   conflict="id")
insert_all("articles",       conflict="id")
insert_all("saved_searches", conflict="id")
insert_all("email_logs",     conflict="id")
insert_all("clients",        conflict="id")
insert_all("client_searches",conflict="id")
insert_all("client_brands",  conflict="id")
insert_all("scrape_queue",   conflict="id")

print("\nDone. Verify on Supabase dashboard before switching DNS/traffic.")
