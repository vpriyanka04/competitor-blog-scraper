import json
import os
import re
import ssl
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import certifi
import feedparser
import requests
import trafilatura
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

# Summarization deps are heavy (sumy pulls in breadability, numpy via its
# dependency chain) and only needed by the UI's Summarize button. Imported
# lazily inside summarize_post so the scrape-job Function can skip them.
try:
    from sumy.nlp.tokenizers import Tokenizer
    from sumy.parsers.plaintext import PlaintextParser
    from sumy.summarizers.lex_rank import LexRankSummarizer
    _SUMY_AVAILABLE = True
except ImportError:
    _SUMY_AVAILABLE = False

# Patch SSL to use certifi bundle (works around missing system certs on locked-down macs)
os.environ.setdefault("SSL_CERT_FILE", certifi.where())
ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())

from db import existing_urls

HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 15
MAX_WORKERS = 8
HEADING_TAGS = ["h1", "h2", "h3", "h4", "h5", "h6"]


def _fetch(url):
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def _to_iso(value):
    if not value:
        return ""
    try:
        return dateparser.parse(value).isoformat()
    except (ValueError, TypeError):
        return ""


def _extract_jsonld(soup):
    for script in soup.find_all("script", type="application/ld+json"):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
        except (json.JSONDecodeError, ValueError):
            continue
        items = data if isinstance(data, list) else [data]
        expanded = []
        for item in items:
            expanded.append(item)
            if isinstance(item, dict) and "@graph" in item:
                expanded.extend(item["@graph"])
        for item in expanded:
            if not isinstance(item, dict):
                continue
            t = item.get("@type")
            if t in ("BlogPosting", "NewsArticle", "Article", "Post"):
                return item
    return None


SUMMARY_SENTENCES = 6
SUMMARY_MAX_CHARS = 1200

# Matches sentences containing concrete data: numbers with units, percentages,
# currency amounts, scale words, or specific years. We prioritize these so the
# summary retains hard metrics rather than abstract arguments.
_DATA_RE = re.compile(
    r"(?<!\w)\d[\d,]*(?:\.\d+)?\s*"
    r"(?:%|percent|x\b|×|million|billion|thousand|k\b|M\b|B\b|"
    r"ms\b|seconds?|minutes?|hours?|days?|weeks?|months?|years?|"
    r"MB|GB|TB|users?|customers?|clients?|companies|developers?|"
    r"requests?|queries|teams?|events?|sessions?|signups?|conversions?|"
    r"crashes?|issues?|bugs?|downloads?|installs?|reviews?|ratings?)",
    re.IGNORECASE,
)
_MONEY_RE = re.compile(r"\$\d[\d,]*(?:\.\d+)?[KMB]?|\d+[\d,]*\s*USD|\d+[\d,]*\s*EUR")
_YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")


def _is_data_sentence(text):
    return bool(_DATA_RE.search(text) or _MONEY_RE.search(text) or _YEAR_RE.search(text))


_SEO_STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "from", "into", "your", "our",
    "you", "they", "them", "their", "here", "there", "what", "why", "how",
    "about", "more", "some", "also", "than", "then", "just", "like", "over",
    "such", "each", "every", "most", "less", "much", "many", "being", "have",
    "has", "had", "was", "were", "are", "been", "can", "will", "would",
}


def _extract_keywords_from_meta(soup):
    keywords = []
    m = soup.find("meta", attrs={"name": "keywords"})
    if m and m.get("content"):
        keywords.extend(k.strip() for k in m["content"].split(",") if k.strip())
    for tag in soup.find_all("meta", attrs={"property": "article:tag"}):
        if tag.get("content"):
            keywords.append(tag["content"].strip())
    for script in soup.find_all("script", type="application/ld+json"):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
        except (json.JSONDecodeError, ValueError):
            continue
        items = data if isinstance(data, list) else [data]
        expanded = list(items)
        for item in items:
            if isinstance(item, dict) and "@graph" in item:
                expanded.extend(item["@graph"])
        for item in expanded:
            if not isinstance(item, dict):
                continue
            if item.get("@type") not in ("BlogPosting", "NewsArticle", "Article", "Post"):
                continue
            kw = item.get("keywords")
            if isinstance(kw, list):
                keywords.extend(str(k).strip() for k in kw)
            elif isinstance(kw, str):
                keywords.extend(k.strip() for k in kw.split(","))
    return keywords


def _dedupe_ci(items):
    seen = set()
    out = []
    for item in items:
        item = item.strip().strip("#").strip(".").strip()
        if not item or len(item) < 4 or len(item) > 70:
            continue
        low = item.lower()
        if low in _SEO_STOPWORDS or low in seen:
            continue
        # skip items where every word is a stopword
        words = low.split()
        if words and all(w in _SEO_STOPWORDS for w in words):
            continue
        seen.add(low)
        out.append(item)
    return out


_NOISE_HEADINGS = {
    "about the author", "where to from here", "get started today",
    "frequently asked questions", "listen to the syntax podcast",
    "bi-weekly intro to sentry demo", "getting started", "related reading",
    "the fix", "table of contents", "enjoying the read",
}

_NOISE_PREFIXES = (
    "enjoying the read", "you may", "check out", "don't miss",
    "listen to", "ready to", "want to", "sign up", "subscribe",
    "follow us", "read more", "try it", "book a demo",
)

_NOISE_TERMS = {
    "min read", "mins read", "minute read", "read more", "read time",
    "pro tip", "latest", "trending", "table of contents",
    "share this", "tweet this", "copy link", "updated on",
}


def _norm_word(w):
    return w.lower().rstrip(":?.!,;\"'")


def _clean_heading_text(t):
    """Strip anchor-link self-repetition and trailing punctuation."""
    t = t.strip().rstrip(":?.!")
    words = t.split()
    n = len(words)
    # 'X Y Z X Y Z' style duplication from anchor permalinks — compare case/punct-insensitive
    if n >= 4 and n % 2 == 0:
        half = n // 2
        if [_norm_word(w) for w in words[:half]] == [_norm_word(w) for w in words[half:]]:
            return " ".join(words[:half]).rstrip(":?.!")
    return t


def _headings_as_keywords(soup):
    """H2/H3 subheadings are the explicit SEO-targeted subsections of a post.
    Scoped to <article>/<main> so we skip sidebar/nav/footer headings."""
    scope = soup.find(["article", "main"]) or soup
    out = []
    for h in scope.find_all(["h2", "h3"]):
        t = _clean_heading_text(h.get_text(" ", strip=True))
        if not (8 <= len(t) <= 90) or len(t.split()) > 9:
            continue
        low = t.lower()
        if low in _NOISE_HEADINGS or any(low.startswith(p) for p in _NOISE_PREFIXES):
            continue
        out.append(t)
    return out


def _yake_phrases(text, n, top, dedup=0.7):
    try:
        import yake
        ex = yake.KeywordExtractor(lan="en", n=n, top=top, dedupLim=dedup)
        return [k for k, _ in ex.extract_keywords(text)]
    except Exception:
        return []


def _trends_candidates(soup, text, max_candidates=20):
    """Build a candidate list of 2-to-4-word phrases only (no single words, no
    long-tail headings). Single words are too generic for SEO/GEO targeting;
    long phrases have no Suggest/Trends signal. YAKE trigrams + bigrams + any
    multi-word author-curated tags."""
    candidates = []
    candidates.extend(_extract_keywords_from_meta(soup))
    if text:
        candidates.extend(_yake_phrases(text, n=3, top=10, dedup=0.75))
        candidates.extend(_yake_phrases(text, n=2, top=14, dedup=0.75))
    filtered = []
    for c in candidates:
        c = c.strip().strip("#.").strip()
        word_count = len(c.split())
        if not (5 <= len(c) <= 60) or not (2 <= word_count <= 4):
            continue
        if c.lower() in _NOISE_TERMS:
            continue
        filtered.append(c)
    return _dedupe_ci(filtered)[:max_candidates]


def _google_suggest_score(query, timeout=5):
    """Ask Google Suggest for autocomplete results. Returns the number of
    suggestions that start with the query — a proxy for 'is this a real,
    popular search term that people google?' Returns 0 on any failure."""
    try:
        r = requests.get(
            "https://suggestqueries.google.com/complete/search",
            params={"client": "firefox", "q": query},
            headers=HEADERS,
            timeout=timeout,
        )
        if r.status_code != 200:
            return 0
        data = r.json()
    except (requests.RequestException, ValueError):
        return 0
    if not (isinstance(data, list) and len(data) >= 2 and isinstance(data[1], list)):
        return 0
    q_low = query.lower()
    return sum(1 for s in data[1] if isinstance(s, str) and s.lower().startswith(q_low))


def _suggest_rank(candidates, min_matches=2):
    """Query Google Suggest for every candidate in parallel. Keep candidates
    with >= min_matches prefix-matched suggestions (i.e. Google recognizes them
    as real searched phrases). Return ordered by match count descending."""
    if not candidates:
        return []
    scores = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_google_suggest_score, c): c for c in candidates}
        for fut in as_completed(futures):
            scores[futures[fut]] = fut.result()
    ranked = [c for c in candidates if scores.get(c, 0) >= min_matches]
    ranked.sort(key=lambda c: scores[c], reverse=True)
    return ranked


def _rank_by_google_trends(keywords, timeframe="today 12-m"):
    """Query Google Trends (via pytrends) for interest score per keyword.
    Returns {keyword: score_0_100}. Returns empty dict if blocked/unavailable."""
    if not keywords:
        return {}
    try:
        from pytrends.request import TrendReq
        pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 25))
    except Exception:
        return {}
    scores = {}
    for i in range(0, len(keywords), 5):
        batch = keywords[i:i + 5]
        try:
            pytrends.build_payload(batch, timeframe=timeframe)
            df = pytrends.interest_over_time()
            if df is not None and not df.empty:
                for kw in batch:
                    if kw in df.columns:
                        scores[kw] = float(df[kw].mean())
            time.sleep(1.5)
        except Exception:
            time.sleep(4)
            continue
    return scores


def fetch_keywords(url, target=10):
    """Return top-N SEO/GEO keywords for a blog post.

    Pipeline:
      1. Extract 2–4-word candidate phrases from meta tags + YAKE trigrams/bigrams.
      2. Filter through Google Suggest — keep only phrases Google autocompletes
         (i.e. real search queries with people-searched-for signal).
      3. Rank by prefix-match count from Suggest (popularity proxy).
      4. Optionally re-rank the survivors through Google Trends (if not blocked).
    """
    try:
        html = _fetch(url)
    except requests.RequestException:
        return []
    soup = BeautifulSoup(html, "lxml")
    text = trafilatura.extract(html, include_comments=False, include_tables=False) or ""

    candidates = _trends_candidates(soup, text)
    if not candidates:
        return []

    searchable = _suggest_rank(candidates, min_matches=2)
    pool = searchable if searchable else candidates

    scores = _rank_by_google_trends(pool[:15])
    if scores and any(v > 0 for v in scores.values()):
        pool = sorted(pool[:15], key=lambda k: scores.get(k, 0.0), reverse=True)

    return pool[:target]


def summarize_post(url):
    """Fetch a blog post, extract the article body, and return a descriptive
    extractive summary. Combines LexRank-selected central sentences with any
    data-bearing sentences (numbers, percentages, currency, years) so metrics
    survive the summarization. Sentences are re-ordered to match document flow."""
    if not _SUMY_AVAILABLE:
        return ""
    try:
        html = _fetch(url)
    except requests.RequestException:
        return ""

    text = trafilatura.extract(
        html,
        include_comments=False,
        include_tables=False,
        favor_recall=True,
    ) or ""

    if text and len(text) > 400:
        try:
            parser = PlaintextParser.from_string(text, Tokenizer("english"))
            summarizer = LexRankSummarizer()
            top = {str(s) for s in summarizer(parser.document, SUMMARY_SENTENCES)}

            all_sentences = [str(s) for s in parser.document.sentences]
            for s in all_sentences:
                if _is_data_sentence(s):
                    top.add(s)

            seen = set()
            ordered = []
            for s in all_sentences:
                if s in top and s not in seen:
                    ordered.append(s)
                    seen.add(s)

            summary = " ".join(ordered).strip()
            if summary:
                if len(summary) > SUMMARY_MAX_CHARS:
                    summary = summary[:SUMMARY_MAX_CHARS].rsplit(" ", 1)[0] + "…"
                return summary
        except Exception:
            pass
        return text[:1800].rsplit(" ", 1)[0] + "…"

    soup = BeautifulSoup(html, "lxml")
    for attrs in (
        {"property": "og:description"},
        {"name": "description"},
        {"name": "twitter:description"},
    ):
        m = soup.find("meta", attrs=attrs)
        if m and m.get("content"):
            return m["content"].strip()[:600]
    return ""


_AEO_WEIGHTS = {
    "question_headings": 1.5,
    "faq_schema": 1.5,
    "data_density": 1.5,
    "article_schema": 1.0,
    "scannability": 1.0,
    "list_usage": 1.0,
    "meta_description": 1.0,
    "author": 0.5,
}
_AEO_QUESTION_PREFIXES = (
    "what ", "how ", "why ", "when ", "where ", "which ", "who ", "will ",
    "can ", "could ", "do ", "does ", "should ", "is ", "are ",
)


def _compute_aeo(soup, text):
    """Heuristic AEO score (1-10) plus per-signal details.
    Each signal dict contains 'score' (0-1) for aggregation PLUS raw metrics
    (counts, lengths, missing fields) used to generate post-specific
    recommendations instead of generic advice."""
    signals = {}
    scope = soup.find(["article", "main"]) or soup
    headings = scope.find_all(["h2", "h3"])

    # 1. Question-based headings
    q_count = 0
    for h in headings:
        t = h.get_text(strip=True).lower()
        if "?" in t or any(t.startswith(p) for p in _AEO_QUESTION_PREFIXES):
            q_count += 1
    total_headings = len(headings)
    q_score = min(1.0, (q_count / total_headings) * 2) if total_headings else 0.0
    signals["question_headings"] = {
        "score": q_score,
        "questions": q_count,
        "total_headings": total_headings,
    }

    # 2. FAQ / HowTo schema
    has_faq_schema = False
    has_blog_schema = False
    blog_fields = 0
    blog_missing = []
    for script in soup.find_all("script", type="application/ld+json"):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
        except (json.JSONDecodeError, ValueError):
            continue
        items = data if isinstance(data, list) else [data]
        expanded = list(items)
        for it in items:
            if isinstance(it, dict) and "@graph" in it:
                expanded.extend(it["@graph"])
        for it in expanded:
            if not isinstance(it, dict):
                continue
            t = it.get("@type")
            if isinstance(t, list):
                t = t[0] if t else None
            if t in ("FAQPage", "HowTo"):
                has_faq_schema = True
            if t in ("BlogPosting", "Article", "NewsArticle"):
                has_blog_schema = True
                req = ("headline", "datePublished", "author", "description")
                filled = [f for f in req if it.get(f)]
                if len(filled) > blog_fields:
                    blog_fields = len(filled)
                    blog_missing = [f for f in req if not it.get(f)]
    has_faq_heading = any(
        "faq" in h.get_text(strip=True).lower() or "frequently asked" in h.get_text(strip=True).lower()
        for h in headings
    )
    signals["faq_schema"] = {
        "score": 1.0 if has_faq_schema else (0.5 if has_faq_heading else 0.0),
        "has_schema": has_faq_schema,
        "has_heading": has_faq_heading,
    }

    # 3. Data density
    sentences = [s for s in re.split(r"(?<=[.!?])\s+", text) if len(s.strip()) > 10]
    total_sentences = len(sentences)
    data_count = sum(1 for s in sentences if _is_data_sentence(s))
    d_score = min(1.0, (data_count / total_sentences) * 5) if total_sentences else 0.0
    signals["data_density"] = {
        "score": d_score,
        "data_sentences": data_count,
        "total_sentences": total_sentences,
    }

    # 4. BlogPosting JSON-LD completeness
    signals["article_schema"] = {
        "score": (blog_fields / 4) if has_blog_schema else 0.0,
        "has_schema": has_blog_schema,
        "fields_filled": blog_fields,
        "fields_missing": blog_missing,
    }

    # 5. Scannability
    para_lens = []
    for p in scope.find_all("p"):
        p_text = p.get_text(" ", strip=True)
        if len(p_text) > 30:
            s_count = max(1, len(re.split(r"(?<=[.!?])\s+", p_text)))
            para_lens.append(s_count)
    if para_lens:
        avg = sum(para_lens) / len(para_lens)
        s_score = max(0.0, min(1.0, (6 - avg) / 4))
    else:
        avg = 0.0
        s_score = 0.0
    signals["scannability"] = {
        "score": s_score,
        "avg_sentences_per_paragraph": round(avg, 1),
        "paragraph_count": len(para_lens),
    }

    # 6. List usage
    lists = scope.find_all(["ul", "ol"])
    list_sizes = [len(l.find_all("li", recursive=False)) for l in lists]
    substantial = sum(1 for n in list_sizes if n >= 3)
    if substantial > 0:
        l_score = 1.0
    elif lists:
        l_score = 0.5
    else:
        l_score = 0.0
    signals["list_usage"] = {
        "score": l_score,
        "total_lists": len(lists),
        "substantial_lists": substantial,
        "max_list_size": max(list_sizes) if list_sizes else 0,
    }

    # 7. Meta description
    desc_length = 0
    for attrs in ({"property": "og:description"}, {"name": "description"}):
        m = soup.find("meta", attrs=attrs)
        if m and m.get("content"):
            desc_length = max(desc_length, len(m["content"].strip()))
    if 50 <= desc_length <= 300:
        m_score = 1.0
    elif desc_length > 0:
        m_score = 0.5
    else:
        m_score = 0.0
    signals["meta_description"] = {
        "score": m_score,
        "present": desc_length > 0,
        "length": desc_length,
    }

    # 8. Author byline
    author_present = False
    author_source = None
    for attrs in ({"name": "author"}, {"property": "article:author"}, {"property": "og:author"}):
        m = soup.find("meta", attrs=attrs)
        if m and m.get("content"):
            author_present = True
            author_source = "meta"
            break
    if not author_present:
        for script in soup.find_all("script", type="application/ld+json"):
            if script.string and '"author"' in script.string:
                author_present = True
                author_source = "jsonld"
                break
    signals["author"] = {
        "score": 1.0 if author_present else 0.0,
        "present": author_present,
        "source": author_source,
    }

    total = sum(_AEO_WEIGHTS.values())  # 9.0
    weighted = sum(signals[k]["score"] * _AEO_WEIGHTS[k] for k in _AEO_WEIGHTS)
    score = 1 + round(9 * weighted / total)
    return max(1, min(10, int(score))), signals


def _analyze_post(url):
    """Fetch a post page once and extract (iso_date, aeo_score, aeo_signals, headline)."""
    try:
        html = _fetch(url)
    except requests.RequestException:
        return "", None, None, None
    soup = BeautifulSoup(html, "lxml")
    iso = ""
    headline = None
    item = _extract_jsonld(soup)
    if item:
        iso = _to_iso(item.get("datePublished", ""))
        headline = item.get("headline")
    if not iso:
        meta = soup.find("meta", property="article:published_time")
        if meta:
            iso = _to_iso(meta.get("content", ""))
    text = trafilatura.extract(html, include_comments=False, include_tables=False) or ""
    score, signals = _compute_aeo(soup, text)
    return iso, score, signals, headline


def _fetch_post_meta(url):
    """Backwards-compat wrapper: returns (iso_date, headline_or_None)."""
    iso, _, _, headline = _analyze_post(url)
    return iso, headline


def compute_aeo_score(url):
    """Public API: fetch a URL and return (score 1-10, signals dict)."""
    _, score, signals, _ = _analyze_post(url)
    return score, signals


def _find_title(anchor, title_selector):
    if title_selector:
        node = anchor.select_one(title_selector)
        if node:
            return node.get_text(strip=True)
    heading = anchor.find(HEADING_TAGS)
    if heading:
        return heading.get_text(strip=True)
    return None


def _enrich_posts(posts):
    """For each post not in the DB, fetch the individual page to extract
    the publication date (if missing) and compute the AEO score + signals.
    Parallelized. Skips posts already in the DB — we already have their data."""
    known = existing_urls([p["url"] for p in posts])
    to_fetch = [p for p in posts if p["url"] not in known]
    if not to_fetch:
        return posts

    def work(post):
        iso, score, signals, _ = _analyze_post(post["url"])
        if iso and not post.get("published_at"):
            post["published_at"] = iso
        if score is not None:
            post["aeo_score"] = score
            post["aeo_signals"] = json.dumps(signals) if signals else None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        list(pool.map(work, to_fetch))
    return posts


# Keep the old name working for any external callers
_enrich_dates = _enrich_posts


def _scrape_html_blog(base_url, list_url, source, href_prefix, title_selector=None):
    html = _fetch(list_url)
    soup = BeautifulSoup(html, "lxml")
    posts = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/"):
            href = urljoin(base_url, href)
        if not href.startswith(href_prefix):
            continue
        if href.rstrip("/") == href_prefix.rstrip("/"):
            continue
        title = _find_title(a, title_selector)
        if not title or href in seen:
            continue
        seen.add(href)
        posts.append({
            "source": source,
            "title": title,
            "url": href,
            "published_at": "",
            "summary": "",
        })
    return _enrich_dates(posts)


def scrape_sentry():
    response = requests.get("https://blog.sentry.io/feed.xml", headers=HEADERS, timeout=TIMEOUT)
    feed = feedparser.parse(response.text)
    posts = []
    for entry in feed.entries:
        posts.append({
            "source": "sentry",
            "title": entry.title,
            "url": entry.link,
            "published_at": _to_iso(entry.get("published", "")),
            "summary": entry.get("summary", "")[:300],
        })
    return _enrich_posts(posts)


def scrape_amplitude():
    return _scrape_html_blog(
        base_url="https://amplitude.com",
        list_url="https://amplitude.com/blog",
        source="amplitude",
        href_prefix="https://amplitude.com/blog/",
    )


def scrape_appbot():
    return _scrape_html_blog(
        base_url="https://appbot.co",
        list_url="https://appbot.co/blog/",
        source="appbot",
        href_prefix="https://appbot.co/blog/",
    )


def scrape_luciq():
    return _scrape_html_blog(
        base_url="https://luciq.ai",
        list_url="https://luciq.ai/blog",
        source="luciq",
        href_prefix="https://luciq.ai/blog/",
        title_selector="div.heading-style-h5",
    )


MIXPANEL_SITEMAP = "https://mixpanel.com/blog/sitemap.xml?sitemap=post-sitemap.xml"
MIXPANEL_LIMIT = 30


def scrape_mixpanel():
    """Mixpanel's blog is a JS-rendered SPA, but individual post pages are SSR-rendered
    with JSON-LD. We discover URLs via the WordPress post sitemap, then fetch each."""
    xml = _fetch(MIXPANEL_SITEMAP)
    soup = BeautifulSoup(xml, "xml")
    entries = []
    for u in soup.find_all("url"):
        loc = u.find("loc")
        lastmod = u.find("lastmod")
        if not loc:
            continue
        url = loc.text.strip()
        if url.rstrip("/") == "https://mixpanel.com/blog":
            continue
        entries.append({"url": url, "lastmod": lastmod.text.strip() if lastmod else ""})
    entries.sort(key=lambda x: x["lastmod"], reverse=True)
    entries = entries[:MIXPANEL_LIMIT]
    known = existing_urls([e["url"] for e in entries])

    def fetch_one(entry):
        if entry["url"] in known:
            return None  # already stored
        iso, score, signals, headline = _analyze_post(entry["url"])
        if not headline:
            return None
        return {
            "source": "mixpanel",
            "title": headline,
            "url": entry["url"],
            "published_at": iso or _to_iso(entry["lastmod"]),
            "summary": "",
            "aeo_score": score,
            "aeo_signals": json.dumps(signals) if signals else None,
        }

    posts = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for result in pool.map(fetch_one, entries):
            if result:
                posts.append(result)
    return posts


def scrape_apptics():
    """Zoho Apptics digest. Each post is an <li> containing an <h3> title and a
    sibling <a> pointing at /apptics/digest/<slug>.html."""
    base = "https://www.zoho.com"
    list_url = "https://www.zoho.com/apptics/digest/?src=apptics-header"
    html = _fetch(list_url)
    soup = BeautifulSoup(html, "lxml")
    posts = []
    seen = set()
    for li in soup.find_all("li"):
        h3 = li.find("h3")
        a = li.find("a", href=True)
        if not h3 or not a:
            continue
        href = a["href"]
        if href.startswith("/"):
            href = urljoin(base, href)
        if "/apptics/digest/" not in href or not href.endswith(".html"):
            continue
        if href in seen:
            continue
        title = h3.get_text(strip=True)
        if not title:
            continue
        seen.add(href)
        posts.append({
            "source": "apptics",
            "title": title,
            "url": href,
            "published_at": "",
            "summary": "",
        })
    return _enrich_dates(posts)


SCRAPERS = [scrape_apptics, scrape_mixpanel, scrape_amplitude, scrape_luciq, scrape_sentry, scrape_appbot]
SOURCE_NAMES = ["apptics", "mixpanel", "amplitude", "luciq", "sentry", "appbot"]
