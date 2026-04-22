import json
from datetime import datetime, timedelta

import streamlit as st
from dateutil import parser as dateparser

from concurrent.futures import ThreadPoolExecutor

from catalyst_storage import download_db, upload_db
from db import (
    init_db,
    insert_post,
    list_posts,
    posts_missing_aeo,
    source_counts,
    update_aeo,
    update_keywords,
    update_summary,
)
from scrapers import SCRAPERS, SOURCE_NAMES, compute_aeo_score, fetch_keywords, summarize_post

st.set_page_config(page_title="Competitors Blogs", page_icon="📰", layout="wide")


@st.cache_resource
def _bootstrap_db():
    """Pull the SQLite DB from Catalyst File Store on first app start.
    Cached so we don't re-download on every Streamlit rerun."""
    download_db()
    init_db()
    return True


_bootstrap_db()

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

# ── Custom CSS: dark purple theme with glowing cards ──────
st.markdown(
    """
    <style>
    [data-testid="stSidebar"] { display: none; }

    [data-testid="stAppViewContainer"] > .main {
        background:
            radial-gradient(ellipse at 20% -10%, rgba(139, 92, 246, 0.18) 0%, transparent 55%),
            radial-gradient(ellipse at 80% 110%, rgba(217, 70, 239, 0.10) 0%, transparent 55%),
            linear-gradient(180deg, #120828 0%, #0a0518 100%);
    }

    /* Bordered cards — border-only glow, card interior is flat */
    div[data-testid="stVerticalBlockBorderWrapper"] {
        border-radius: 16px !important;
        background: transparent !important;
        border: 1px solid rgba(192, 132, 252, 0.50) !important;
        box-shadow:
            0 0 0 1px rgba(192, 132, 252, 0.20),
            0 0 6px rgba(192, 132, 252, 0.45),
            0 0 12px rgba(217, 70, 239, 0.22);
        padding: 0.6rem !important;
        margin-bottom: 14px;
        transition: border-color 140ms ease, box-shadow 140ms ease;
    }
    div[data-testid="stVerticalBlockBorderWrapper"]:hover {
        border-color: rgba(233, 213, 255, 0.75) !important;
        box-shadow:
            0 0 0 1px rgba(233, 213, 255, 0.35),
            0 0 8px rgba(233, 213, 255, 0.60),
            0 0 16px rgba(217, 70, 239, 0.40);
    }

    /* Primary buttons (Refresh + active source) — matt purple, glow only on border */
    button[kind="primary"] {
        background: #7e22ce !important;
        border: 1px solid #c084fc !important;
        color: white !important;
        box-shadow:
            0 0 0 1px rgba(192, 132, 252, 0.40),
            0 0 6px rgba(192, 132, 252, 0.50),
            0 0 12px rgba(192, 132, 252, 0.25) !important;
    }
    button[kind="primary"]:hover {
        background: #9333ea !important;
        border-color: #e9d5ff !important;
        box-shadow:
            0 0 0 1px rgba(233, 213, 255, 0.55),
            0 0 8px rgba(233, 213, 255, 0.55),
            0 0 14px rgba(233, 213, 255, 0.30) !important;
    }

    /* Secondary buttons (unselected source boxes) — flat matt, no glow */
    button[kind="secondary"] {
        background: rgba(88, 28, 135, 0.35) !important;
        border: 1px solid rgba(192, 132, 252, 0.25) !important;
        color: #E0D4F7 !important;
        box-shadow: none !important;
    }
    button[kind="secondary"]:hover {
        background: rgba(126, 34, 206, 0.45) !important;
        border-color: rgba(192, 132, 252, 0.50) !important;
        color: #FFFFFF !important;
        box-shadow:
            0 0 0 1px rgba(192, 132, 252, 0.35),
            0 0 6px rgba(192, 132, 252, 0.35) !important;
    }

    /* Link buttons "Read →" */
    [data-testid="stLinkButton"] a {
        background: linear-gradient(135deg, rgba(168, 85, 247, 0.18), rgba(217, 70, 239, 0.14)) !important;
        border: 1px solid rgba(192, 132, 252, 0.35) !important;
        color: #EDE4FA !important;
    }

    .stAlert { border-radius: 12px; }

    /* Title gradient — vibrant violet → fuchsia */
    h1 {
        background: linear-gradient(135deg, #e9d5ff, #f0abfc, #c084fc);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }

    /* Post title inside cards */
    div[data-testid="stVerticalBlockBorderWrapper"] h3 a {
        color: #EDE4FA !important;
        text-decoration: none;
    }
    div[data-testid="stVerticalBlockBorderWrapper"] h3 a:hover {
        color: #e9d5ff !important;
    }

    /* Search input purple tint */
    [data-baseweb="input"] > div {
        background: rgba(88, 28, 135, 0.18) !important;
        border-color: rgba(192, 132, 252, 0.25) !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if "source" not in st.session_state:
    st.session_state.source = "All"

counts_map = {c["source"]: c["n"] for c in source_counts()}

# ── Title row + Refresh at top-right ──────────────────────
title_col, spacer_col, btn_col = st.columns([7, 1, 1])
with title_col:
    st.title("Competitors Blogs")
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
            "background:linear-gradient(135deg,#e9d5ff,#c084fc);"
            "-webkit-background-clip:text;-webkit-text-fill-color:transparent;"
            "background-clip:text;margin:4px 0 14px 0;'>Keywords</div>",
            unsafe_allow_html=True,
        )
        if not keywords:
            st.info("No keywords could be extracted for this post.")
            return
        pills = "".join(
            f"<span style='display:inline-block;margin:4px 6px 4px 0;padding:6px 14px;"
            f"background:rgba(88,28,135,0.35);"
            f"border:1px solid rgba(192,132,252,0.45);"
            f"border-radius:999px;color:#EDE4FA;font-size:0.88rem;"
            f"box-shadow:0 0 0 1px rgba(192,132,252,0.20), 0 0 6px rgba(192,132,252,0.35);'>"
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
                        "background:linear-gradient(135deg,#c084fc,#d946ef);"
                        "color:#fff;font-size:0.62rem;font-weight:700;"
                        "letter-spacing:0.08em;text-transform:uppercase;"
                        "border-radius:999px;"
                        "box-shadow:0 0 0 1px rgba(233,213,255,0.4),"
                        "0 0 6px rgba(217,70,239,0.35);'>New</span>"
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
                            "background:rgba(88,28,135,0.10);"
                            "border-left:3px solid #c084fc;"
                            "border-radius:8px;'>"
                            "<summary style='cursor:pointer;font-weight:700;"
                            "text-transform:uppercase;letter-spacing:0.06em;"
                            "font-size:0.74rem;list-style:none;"
                            "background:linear-gradient(135deg,#e9d5ff,#c084fc);"
                            "-webkit-background-clip:text;"
                            "-webkit-text-fill-color:transparent;"
                            "background-clip:text;'>"
                            "How to increase the content score"
                            "</summary>"
                            "<ul style='margin:10px 0 2px 0;padding-left:18px;"
                            "color:#CAD5E5;font-size:0.87rem;line-height:1.55;'>"
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
                    "background:rgba(88, 28, 135, 0.08);border-left:3px solid #c084fc;"
                    "border-radius:8px;color:#EDE4FA;font-size:0.92rem;line-height:1.55;'>"
                    "<div style='display:flex;align-items:center;gap:8px;"
                    "font-weight:700;font-size:0.82rem;letter-spacing:0.06em;"
                    "text-transform:uppercase;"
                    "background:linear-gradient(135deg,#e9d5ff,#c084fc);"
                    "-webkit-background-clip:text;-webkit-text-fill-color:transparent;"
                    "background-clip:text;margin-bottom:8px;'>"
                    "<svg width='16' height='16' viewBox='0 0 24 24' fill='none' "
                    "xmlns='http://www.w3.org/2000/svg' style='flex-shrink:0;'>"
                    "<path d='M14 3v4a1 1 0 0 0 1 1h4' stroke='#c084fc' stroke-width='2' "
                    "stroke-linecap='round' stroke-linejoin='round'/>"
                    "<path d='M17 21H7a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h7l5 5v11a2 2 0 0 1-2 2z' "
                    "stroke='#c084fc' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'/>"
                    "<path d='M9 13h6M9 17h6M9 9h2' stroke='#c084fc' stroke-width='2' "
                    "stroke-linecap='round'/>"
                    "</svg>"
                    "<span>Summary</span>"
                    "</div>"
                    f"<div>{post['summary']}</div>"
                    "</div>",
                    unsafe_allow_html=True,
                )
