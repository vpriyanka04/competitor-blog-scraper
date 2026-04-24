import json
from datetime import datetime, timedelta

import streamlit as st
from dateutil import parser as dateparser

from concurrent.futures import ThreadPoolExecutor

from catalyst_storage import download_db, upload_db
from db import (
    init_db,
    insert_post,
    latest_fetched_at,
    list_posts,
    posts_missing_aeo,
    source_counts,
    update_aeo,
    update_keywords,
    update_summary,
)
from scrapers import SCRAPERS, SOURCE_NAMES, compute_aeo_score, fetch_keywords, summarize_post

st.set_page_config(page_title="Competitors Blogs", page_icon="📰", layout="wide")

if "apptics_injected" not in st.session_state:
    st.html(
        '<script type="text/javascript" id="zohoapptics">'
        'var d=document,s=d.createElement("script");'
        's.type="text/javascript";s.id="zohoappticsscript";s.defer=!0;'
        's.src="https://apptics.zoho.in/sdk/web/v1/60047108145/654000017429321/init?aaID=654146762056265";'
        'var t=d.getElementsByTagName("script")[0];t.parentNode.insertBefore(s,t);'
        'window.appticsReady=function(s){var e=window.apptics__asyncalls=window.apptics__asyncalls||[];'
        'window.appticsReadyStatus?(s&&e.push(s),e.forEach(s=>s&&s()),window.apptics__asyncalls=null):s&&e.push(s)};'
        "</script>"
    )
    st.session_state["apptics_injected"] = True


AUTO_REFRESH_MAX_AGE_HOURS = 24


@st.cache_resource
def _bootstrap_db():
    """Pull the SQLite DB from Catalyst File Store on first app start.
    Cached so we don't re-download on every Streamlit rerun."""
    download_db()
    init_db()
    return True


def _auto_refresh_if_stale():
    """If the newest fetched_at is older than AUTO_REFRESH_MAX_AGE_HOURS
    (or the DB is empty), silently run all scrapers before rendering.
    Other visitors during the same window see cached (fresh) data for free."""
    last = latest_fetched_at()
    stale = True
    if last:
        try:
            last_dt = dateparser.parse(last).replace(tzinfo=None)
            age_hours = (datetime.utcnow() - last_dt).total_seconds() / 3600
            stale = age_hours > AUTO_REFRESH_MAX_AGE_HOURS
        except (ValueError, TypeError):
            stale = True
    if not stale:
        return
    with st.spinner("Loading latest posts across all sources (~45s)…"):
        for scrape in SCRAPERS:
            try:
                for post in scrape():
                    insert_post(post)
            except Exception:
                pass
    upload_db()  # no-op when not running on Catalyst


def _humanize_ago(iso):
    try:
        dt = dateparser.parse(iso).replace(tzinfo=None)
        delta = datetime.utcnow() - dt
        seconds = delta.total_seconds()
        if seconds < 60:
            return "just now"
        if seconds < 3600:
            return f"{int(seconds // 60)} min ago"
        if seconds < 86400:
            return f"{int(seconds // 3600)} hr ago"
        days = int(seconds // 86400)
        return f"{days} day{'s' if days != 1 else ''} ago"
    except (ValueError, TypeError):
        return iso


_bootstrap_db()
_auto_refresh_if_stale()

SOURCE_LABELS = {
    "sentry": "Sentry",
    "amplitude": "Amplitude",
    "appbot": "Appbot",
    "luciq": "Luciq (Instabug)",
    "mixpanel": "Mixpanel",
    "apptics": "Zoho Apptics",
}

AEO_WEIGHTS = {
    "question_headings": 1.5,
    "faq_schema": 1.5,
    "data_density": 1.5,
    "article_schema": 1.0,
    "scannability": 1.0,
    "list_usage": 1.0,
    "meta_description": 1.0,
    "author": 0.5,
}


def _aeo_recommendation(signal, detail):
    """Build a post-specific recommendation that cites this post's actual numbers
    (heading counts, paragraph lengths, missing schema fields, etc.)."""
    if signal == "question_headings":
        q = detail.get("questions", 0)
        total = detail.get("total_headings", 0)
        if total == 0:
            return ("This post has no H2/H3 subheadings at all. Add 4–8 "
                    "question-shaped subheadings so answer engines have "
                    "anchors to match user queries against.")
        target = max(1, total // 2 + 1 - q)
        return (f"Only <b>{q} of {total}</b> subheadings are question-shaped. "
                f"Rewrite at least <b>{target} more</b> as questions "
                f"(\"What is…?\", \"How to…?\") — question-headings are the "
                "anchors LLMs match user queries against.")
    if signal == "faq_schema":
        if detail.get("has_heading") and not detail.get("has_schema"):
            return ("You have a \"Frequently Asked Questions\" section but no "
                    "<code>FAQPage</code> JSON-LD wrapping it. Add the schema "
                    "— the heading alone is invisible to engines; the schema "
                    "lets them parse Q→A pairs directly.")
        return ("No FAQ section on this post. Add 4–6 common user questions "
                "with concise answers, wrapped in <code>FAQPage</code> JSON-LD. "
                "Single strongest signal for direct-answer extraction.")
    if signal == "data_density":
        got = detail.get("data_sentences", 0)
        total = detail.get("total_sentences", 0)
        if total == 0:
            return ("Couldn't detect article body text. Make sure the post "
                    "has readable paragraph content, then aim for ≥20% of "
                    "sentences to include specific numbers or metrics.")
        target = max(1, int(total * 0.20) - got)
        return (f"Only <b>{got} of {total} sentences</b> ({int(100*got/total)}%) "
                f"contain concrete data. Add at least <b>{target} more</b> "
                "stat-bearing sentences (percentages, user counts, time "
                "metrics). LLMs preferentially cite specific numbers.")
    if signal == "article_schema":
        if not detail.get("has_schema"):
            return ("No <code>BlogPosting</code> (or <code>Article</code>) "
                    "JSON-LD on the page. Add one with headline, "
                    "datePublished, author, and description — engines rely "
                    "on this to decide whether your content is citation-worthy.")
        missing = detail.get("fields_missing", [])
        filled = detail.get("fields_filled", 0)
        miss_str = ", ".join(f"<code>{m}</code>" for m in missing) or "—"
        return (f"<code>BlogPosting</code> JSON-LD has only "
                f"<b>{filled}/4</b> key fields. Add the missing: {miss_str}.")
    if signal == "scannability":
        avg = detail.get("avg_sentences_per_paragraph", 0)
        return (f"Avg paragraph length is <b>{avg} sentences</b> (target: "
                "≤ 3). Break long paragraphs into short ones — answer "
                "engines extract short blocks and skip walls of text.")
    if signal == "list_usage":
        total_lists = detail.get("total_lists", 0)
        substantial = detail.get("substantial_lists", 0)
        max_size = detail.get("max_list_size", 0)
        if total_lists == 0:
            return ("No bullet or numbered lists in this post. Add a "
                    "\"Key takeaways\" list or convert related sentences "
                    "into a 5–7 item list. Lists are prime extraction "
                    "targets for AI overviews.")
        if substantial == 0:
            return (f"You have <b>{total_lists}</b> list(s), but the "
                    f"largest has only <b>{max_size} item(s)</b>. Expand "
                    "to 3+ items each — shorter lists don't register as "
                    "list-shaped content to engines.")
        return ("Add one more substantial list (5+ items) to increase "
                "extractable snippets.")
    if signal == "meta_description":
        length = detail.get("length", 0)
        if length == 0:
            return ("Missing <code>&lt;meta name=\"description\"&gt;</code> "
                    "tag. Add a 50–160 char summary — engines quote this "
                    "directly in snippets and AI previews.")
        return (f"Meta description is <b>{length} chars</b> — outside the "
                "50–160 char sweet spot. Tighten (or lengthen) to fit "
                "engine snippet rendering.")
    if signal == "author":
        return ("No author information detected — no <code>author</code> "
                "meta tag, no JSON-LD author field. Add an author byline "
                "with name and ideally a linked bio page. E-E-A-T trust "
                "signals raise citation likelihood.")
    return None

# ── Custom CSS: deep-navy + warm coral accent (Syself-inspired) ────
st.markdown(
    """
    <style>
    [data-testid="stSidebar"] { display: none; }

    [data-testid="stAppViewContainer"] > .main {
        background:
            radial-gradient(ellipse at 10% -10%, rgba(255, 133, 102, 0.10) 0%, transparent 55%),
            radial-gradient(ellipse at 90% 110%, rgba(239, 109, 79, 0.08) 0%, transparent 55%),
            linear-gradient(180deg, #0e1034 0%, #0a0d28 100%);
    }

    /* Cards — subtle navy with soft coral-tinted border */
    div[data-testid="stVerticalBlockBorderWrapper"] {
        border-radius: 14px !important;
        background: rgba(23, 26, 64, 0.55) !important;
        border: 1px solid rgba(255, 133, 102, 0.18) !important;
        box-shadow: 0 1px 0 rgba(255, 255, 255, 0.02) inset;
        padding: 0.6rem !important;
        margin-bottom: 14px;
        transition: border-color 160ms ease, background 160ms ease, transform 160ms ease;
    }
    div[data-testid="stVerticalBlockBorderWrapper"]:hover {
        border-color: rgba(255, 145, 112, 0.45) !important;
        background: rgba(28, 32, 74, 0.70) !important;
        transform: translateY(-1px);
    }

    /* Primary buttons — warm coral gradient CTA */
    button[kind="primary"] {
        background: linear-gradient(135deg, #ffb6a1 0%, #ff9170 50%, #ef6d4f 100%) !important;
        border: none !important;
        color: #1a0d0a !important;
        font-weight: 600 !important;
    }
    button[kind="primary"]:hover {
        background: linear-gradient(135deg, #ffc9b4 0%, #ffa488 50%, #f37e60 100%) !important;
        box-shadow: 0 4px 16px rgba(239, 109, 79, 0.30) !important;
    }

    /* Secondary buttons — navy with coral border on hover */
    button[kind="secondary"] {
        background: rgba(23, 26, 64, 0.70) !important;
        border: 1px solid rgba(255, 180, 160, 0.18) !important;
        color: #cfd1e0 !important;
        box-shadow: none !important;
    }
    button[kind="secondary"]:hover {
        background: rgba(33, 37, 82, 0.85) !important;
        border-color: rgba(255, 145, 112, 0.55) !important;
        color: #ffffff !important;
    }

    /* Link buttons "Read →" — subtle navy with coral border */
    [data-testid="stLinkButton"] a {
        background: rgba(23, 26, 64, 0.70) !important;
        border: 1px solid rgba(255, 180, 160, 0.25) !important;
        color: #ebecf5 !important;
    }
    [data-testid="stLinkButton"] a:hover {
        border-color: rgba(255, 145, 112, 0.70) !important;
        background: rgba(33, 37, 82, 0.90) !important;
    }

    .stAlert { border-radius: 12px; }

    /* Title — coral sunset gradient */
    h1 {
        background: linear-gradient(135deg, #ffb6a1 0%, #ff9170 45%, #ef6d4f 90%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-weight: 700;
    }

    /* Post title inside cards */
    div[data-testid="stVerticalBlockBorderWrapper"] h3 a {
        color: #ebecf5 !important;
        text-decoration: none;
    }
    div[data-testid="stVerticalBlockBorderWrapper"] h3 a:hover {
        color: #ffb6a1 !important;
    }

    /* Search input */
    [data-baseweb="input"] > div {
        background: rgba(23, 26, 64, 0.70) !important;
        border-color: rgba(255, 180, 160, 0.18) !important;
    }
    [data-baseweb="input"] > div:focus-within {
        border-color: rgba(255, 145, 112, 0.65) !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if "source" not in st.session_state:
    st.session_state.source = "All"

counts_map = {c["source"]: c["n"] for c in source_counts()}

# ── Title row + Refresh at top-right ──────────────────────
title_col, meta_col, btn_col = st.columns([7, 1.4, 1])
with title_col:
    st.title("Competitors Blogs")
with meta_col:
    st.write("")
    st.write("")
    _last = latest_fetched_at()
    if _last:
        st.caption(f"Last updated {_humanize_ago(_last)}")
with btn_col:
    st.write("")
    st.write("")
    refresh_clicked = st.button("Refresh", use_container_width=True, type="primary")

if refresh_clicked:
    with st.spinner("Fetching from all sources (20–40s first run)…"):
        total = 0
        for scrape in SCRAPERS:
            try:
                posts = scrape()
                for post in posts:
                    insert_post(post)
                total += len(posts)
            except Exception as e:
                st.info(f"{scrape.__name__} failed: {e}")
        # Backfill AEO scores for any rows still missing them (legacy data)
        missing = posts_missing_aeo()
        if missing:
            def _backfill(row):
                score, signals = compute_aeo_score(row["url"])
                if score is not None:
                    update_aeo(row["id"], score, json.dumps(signals) if signals else None)
            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(_backfill, missing))
        upload_db()  # push changes up to Catalyst File Store (no-op locally)
        st.success(f"Fetched {total} new posts. Scored {len(missing)} existing posts.")
    st.rerun()

# ── Source selector boxes ─────────────────────────────────
cols = st.columns(7)
box_keys = ["All"] + SOURCE_NAMES
for col, key in zip(cols, box_keys):
    with col:
        label = "All sources" if key == "All" else SOURCE_LABELS[key]
        count = sum(counts_map.values()) if key == "All" else counts_map.get(key, 0)
        is_active = st.session_state.source == key
        btn_label = f"{label}\n\n{count} posts"
        if st.button(
            btn_label,
            key=f"src_{key}",
            use_container_width=True,
            type="primary" if is_active else "secondary",
        ):
            st.session_state.source = key
            st.rerun()

# ── Search ────────────────────────────────────────────────
search = st.text_input("Search", placeholder="Search titles", label_visibility="collapsed")

st.divider()


def show_keywords_dialog(title, keywords):
    """Dialog title is the blog title; body is a 'Keywords' header + pills."""
    @st.dialog(title)
    def _render():
        st.markdown(
            "<div style='font-weight:700;font-size:0.82rem;letter-spacing:0.06em;"
            "text-transform:uppercase;"
            "background:linear-gradient(135deg,#ffb6a1,#ef6d4f);"
            "-webkit-background-clip:text;-webkit-text-fill-color:transparent;"
            "background-clip:text;margin:4px 0 14px 0;'>Keywords</div>",
            unsafe_allow_html=True,
        )
        if not keywords:
            st.info("No keywords could be extracted for this post.")
            return
        pills = "".join(
            f"<span style='display:inline-block;margin:4px 6px 4px 0;padding:6px 14px;"
            f"background:rgba(23,26,64,0.80);"
            f"border:1px solid rgba(255,145,112,0.40);"
            f"border-radius:999px;color:#ebecf5;font-size:0.88rem;'>"
            f"{k}</span>"
            for k in keywords
        )
        st.markdown(f"<div>{pills}</div>", unsafe_allow_html=True)
    _render()


NEW_BADGE_WINDOW_DAYS = 7
_NOW_UTC = datetime.utcnow()


def fmt_date(iso):
    if not iso:
        return "—"
    try:
        return dateparser.parse(iso).strftime("%b %d, %Y")
    except (ValueError, TypeError):
        return iso


def is_new_post(post):
    """Show 'New' badge if the post was fetched within the last NEW_BADGE_WINDOW_DAYS."""
    fetched = post.get("fetched_at")
    if not fetched:
        return False
    try:
        ts = dateparser.parse(fetched)
    except (ValueError, TypeError):
        return False
    ts = ts.replace(tzinfo=None)
    return (_NOW_UTC - ts) < timedelta(days=NEW_BADGE_WINDOW_DAYS)


# ── Post list ─────────────────────────────────────────────
posts = list_posts(source=st.session_state.source, search=search)
header_source = "All sources" if st.session_state.source == "All" else SOURCE_LABELS[st.session_state.source]
st.subheader(f"{header_source} — {len(posts)} posts")

show_source = st.session_state.source == "All"

if not posts:
    st.info("No posts yet. Click Refresh above.")
else:
    for post in posts:
        date_str = fmt_date(post.get("published_at"))
        src_label = SOURCE_LABELS.get(post["source"], post["source"])
        with st.container(border=True):
            top_left, top_right = st.columns([6, 1.2])
            with top_left:
                new_badge = ""
                if is_new_post(post):
                    new_badge = (
                        "&nbsp;&nbsp;<span style='display:inline-block;"
                        "padding:2px 9px;vertical-align:middle;"
                        "background:linear-gradient(135deg,#ffb6a1,#ef6d4f);"
                        "color:#1a0d0a;font-size:0.62rem;font-weight:700;"
                        "letter-spacing:0.08em;text-transform:uppercase;"
                        "border-radius:999px;'>New</span>"
                    )
                st.markdown(
                    f"### [{post['title']}]({post['url']}){new_badge}",
                    unsafe_allow_html=True,
                )
                aeo = post.get("aeo_score")
                score_text = f"  ·  Content score {aeo}/10" if aeo else ""
                if show_source:
                    st.caption(f"**{src_label}**  ·  {date_str}{score_text}")
                else:
                    st.caption(f"{date_str}{score_text}")

                if aeo and aeo < 7 and post.get("aeo_signals"):
                    try:
                        signals = json.loads(post["aeo_signals"])
                    except (ValueError, TypeError):
                        signals = {}
                    gaps = []
                    for key, detail in signals.items():
                        # Accept both old (float score) and new (dict) formats
                        if isinstance(detail, dict):
                            score_val = detail.get("score", 0.0)
                        else:
                            score_val = float(detail)
                            detail = {"score": score_val}
                        if score_val < 0.6:
                            gain = AEO_WEIGHTS.get(key, 1.0) * (1 - score_val)
                            rec = _aeo_recommendation(key, detail)
                            if rec:
                                gaps.append((gain, rec))
                    gaps.sort(key=lambda g: g[0], reverse=True)
                    recs = [r for _, r in gaps[:4]]
                    if recs:
                        items = "".join(
                            f"<li style='margin-bottom:8px;'>{r}</li>" for r in recs
                        )
                        st.markdown(
                            "<details style='margin-top:10px;padding:10px 14px;"
                            "background:rgba(239,109,79,0.06);"
                            "border-left:3px solid #ef6d4f;"
                            "border-radius:8px;'>"
                            "<summary style='cursor:pointer;font-weight:700;"
                            "text-transform:uppercase;letter-spacing:0.06em;"
                            "font-size:0.74rem;list-style:none;"
                            "background:linear-gradient(135deg,#ffb6a1,#ef6d4f);"
                            "-webkit-background-clip:text;"
                            "-webkit-text-fill-color:transparent;"
                            "background-clip:text;'>"
                            "How to increase the content score"
                            "</summary>"
                            "<ul style='margin:10px 0 2px 0;padding-left:18px;"
                            "color:#cfd1e0;font-size:0.87rem;line-height:1.55;'>"
                            f"{items}"
                            "</ul>"
                            "</details>",
                            unsafe_allow_html=True,
                        )
            sum_state_key = f"sum_open_{post['id']}"
            with top_right:
                st.link_button("Read →", post["url"], use_container_width=True)
                if st.button("Summarize", key=f"sum_{post['id']}", use_container_width=True):
                    if post.get("summary"):
                        st.session_state[sum_state_key] = not st.session_state.get(sum_state_key, False)
                    else:
                        with st.spinner("Summarizing…"):
                            summary = summarize_post(post["url"])
                            update_summary(post["id"], summary)
                        st.session_state[sum_state_key] = True
                    st.rerun()
                if st.button("Keywords", key=f"kw_{post['id']}", use_container_width=True):
                    existing = post.get("keywords")
                    try:
                        kws = json.loads(existing) if existing else []
                    except (ValueError, TypeError):
                        kws = []
                    if not kws:
                        with st.spinner("Extracting keywords…"):
                            kws = fetch_keywords(post["url"])
                            update_keywords(post["id"], json.dumps(kws))
                    show_keywords_dialog(post["title"], kws)
            if post.get("summary") and st.session_state.get(sum_state_key, False):
                st.markdown(
                    "<div style='margin-top:10px;padding:12px 16px;"
                    "background:rgba(239,109,79,0.06);border-left:3px solid #ef6d4f;"
                    "border-radius:8px;color:#ebecf5;font-size:0.92rem;line-height:1.55;'>"
                    "<div style='display:flex;align-items:center;gap:8px;"
                    "font-weight:700;font-size:0.82rem;letter-spacing:0.06em;"
                    "text-transform:uppercase;"
                    "background:linear-gradient(135deg,#ffb6a1,#ef6d4f);"
                    "-webkit-background-clip:text;-webkit-text-fill-color:transparent;"
                    "background-clip:text;margin-bottom:8px;'>"
                    "<svg width='16' height='16' viewBox='0 0 24 24' fill='none' "
                    "xmlns='http://www.w3.org/2000/svg' style='flex-shrink:0;'>"
                    "<path d='M14 3v4a1 1 0 0 0 1 1h4' stroke='#ef6d4f' stroke-width='2' "
                    "stroke-linecap='round' stroke-linejoin='round'/>"
                    "<path d='M17 21H7a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h7l5 5v11a2 2 0 0 1-2 2z' "
                    "stroke='#ef6d4f' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'/>"
                    "<path d='M9 13h6M9 17h6M9 9h2' stroke='#ef6d4f' stroke-width='2' "
                    "stroke-linecap='round'/>"
                    "</svg>"
                    "<span>Summary</span>"
                    "</div>"
                    f"<div>{post['summary']}</div>"
                    "</div>",
                    unsafe_allow_html=True,
                )
