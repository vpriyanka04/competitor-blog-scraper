import os
import sqlite3
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "scrapper.db")

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                source        TEXT    NOT NULL,
                title         TEXT    NOT NULL,
                url           TEXT    UNIQUE NOT NULL,
                published_at  TEXT,
                summary       TEXT,
                fetched_at    TEXT    NOT NULL
            )
        """)
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(posts)")}
        if "keywords" not in cols:
            conn.execute("ALTER TABLE posts ADD COLUMN keywords TEXT")
        if "aeo_score" not in cols:
            conn.execute("ALTER TABLE posts ADD COLUMN aeo_score INTEGER")
        if "aeo_signals" not in cols:
            conn.execute("ALTER TABLE posts ADD COLUMN aeo_signals TEXT")

def insert_post(post):
    params = {
        "source": post["source"],
        "title": post["title"],
        "url": post["url"],
        "published_at": post.get("published_at", ""),
        "summary": post.get("summary", ""),
        "fetched_at": datetime.utcnow().isoformat(),
        "aeo_score": post.get("aeo_score"),
        "aeo_signals": post.get("aeo_signals"),
    }
    with get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO posts
              (source, title, url, published_at, summary, fetched_at, aeo_score, aeo_signals)
            VALUES
              (:source, :title, :url, :published_at, :summary, :fetched_at, :aeo_score, :aeo_signals)
        """, params)

def list_posts(source=None, search=None):
    query = "SELECT * FROM posts WHERE 1=1"
    params = []
    if source and source != "All":
        query += " AND source = ?"
        params.append(source)
    if search:
        query += " AND title LIKE ?"
        params.append(f"%{search}%")
    query += " ORDER BY COALESCE(published_at, fetched_at) DESC"
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

def update_summary(post_id, summary):
    with get_conn() as conn:
        conn.execute("UPDATE posts SET summary = ? WHERE id = ?", (summary, post_id))

def update_keywords(post_id, keywords_json):
    with get_conn() as conn:
        conn.execute("UPDATE posts SET keywords = ? WHERE id = ?", (keywords_json, post_id))

def update_aeo(post_id, score, signals_json):
    with get_conn() as conn:
        conn.execute(
            "UPDATE posts SET aeo_score = ?, aeo_signals = ? WHERE id = ?",
            (score, signals_json, post_id),
        )

def posts_missing_aeo():
    with get_conn() as conn:
        rows = conn.execute("SELECT id, url FROM posts WHERE aeo_score IS NULL").fetchall()
        return [dict(r) for r in rows]

def existing_urls(urls):
    if not urls:
        return set()
    placeholders = ",".join("?" * len(urls))
    with get_conn() as conn:
        rows = conn.execute(f"SELECT url FROM posts WHERE url IN ({placeholders})", list(urls)).fetchall()
        return {row["url"] for row in rows}

def source_counts():
    with get_conn() as conn:
        rows = conn.execute("SELECT source, COUNT(*) AS n FROM posts GROUP BY source ORDER BY source").fetchall()
        return [dict(row) for row in rows]
