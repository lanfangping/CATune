#!/usr/bin/env python3
import argparse
import hashlib
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


DEFAULT_MODEL = "deepseek-chat"
DEFAULT_API_URL = "https://api.deepseek.com/v1"
DEFAULT_RUNTIME_CONFIG_URL = "https://www.postgresql.org/docs/13/runtime-config.html"
DEFAULT_USER_AGENT = "constraint-extractor/1.0 (+https://www.postgresql.org/docs/13/runtime-config.html)"


RELATION_LABELS = [
    "bounded by",
    "consider adjusting",
    "defaults proportional to",
    "defaults to",
    "fallback to",
    "greater than or equal to",
    "interacts with",
    "less than",
    "less than or equal to half",
    "multiplied by",
    "requires",
    "requires enabled",
    "requires larger",
    "same or higher",
    "smaller than or equal to",
    "subset of",
    "used in calculation",
    "works with",
]

RELATION_SYNONYMS = {
    "at least": "greater than or equal to",
    "at least as large as": "greater than or equal to",
    "at least as big as": "greater than or equal to",
    "greater than": "greater than or equal to",
    "less than": "less than",
    "smaller than": "less than",
    "no effect if higher than": "bounded by",
    "no effect if greater than": "bounded by",
    "limited by": "bounded by",
    "bounded by": "bounded by",
    "same or higher": "same or higher",
    "defaults to": "defaults to",
    "default to": "defaults to",
    "fallback to": "fallback to",
    "fall back to": "fallback to",
    "works with": "works with",
    "interacts with": "interacts with",
    "requires": "requires",
    "requires enabled": "requires enabled",
    "requires larger": "requires larger",
    "used in calculation": "used in calculation",
    "multiplied by": "multiplied by",
    "subset of": "subset of",
    "consider adjusting": "consider adjusting",
    "defaults proportional to": "defaults proportional to",
    "smaller than or equal to": "smaller than or equal to",
    "less than or equal to half": "less than or equal to half",
}

TRIGGER_TERMS = [
    "must",
    "must be",
    "must not",
    "should",
    "should be",
    "requires",
    "require",
    "required",
    "cannot",
    "cannot be",
    "cannot exceed",
    "not allowed",
    "no effect",
    "has no effect",
    "ignored unless",
    "ignored",
    "only if",
    "only when",
    "if",
    "when",
    "at least",
    "at most",
    "no more than",
    "no less than",
    "less than",
    "less than or equal to",
    "greater than",
    "greater than or equal to",
    "smaller than",
    "larger than",
    "not exceed",
    "limited to",
    "limited by",
    "bounded by",
    "bound by",
    "subset of",
    "depends on",
    "dependent on",
    "taken from",
    "computed as",
    "calculated as",
    "derived from",
    "product of",
    "multiplied by",
    "proportional to",
    "defaults to",
    "default",
    "fallback",
    "fall back",
    "set to",
    "set at",
    "trigger",
    "set the same as",
    "based on",
]


SYSTEM_PROMPT = (
    "You extract PostgreSQL knob constraints from documentation text. "
    "Return ONLY a JSON array, no markdown and no extra text. "
    "Each item must follow this schema: "
    "{"
    "\"knob1\": string, "
    "\"relation\": string (must be one of the allowed labels), "
    "\"knob2\": string, "
    "\"condition\": string (optional), "
    "\"context\": string (short summary), "
    "\"evidence_span\": string (verbatim quote from snippet)"
    "}. "
    "If no explicit knob dependency exists, return []. "
    "Use only information in the provided snippet. "
    "Do not invent knobs or relations. "
    "For standby mirror statements like "
    "\"same or higher value on standby\", use knob2 = standby.<primary_knob>."
)

REFLECTION_SYSTEM_PROMPT = (
    "You are a strict reviewer for PostgreSQL knob-ordering extraction. "
    "Given a snippet and preliminary tuples, remove unsupported tuples, "
    "fix obvious direction errors, and keep only relations supported by explicit evidence. "
    "Return ONLY a JSON array with the same tuple schema. "
    "Do not add external knowledge."
)

JUDGE_SYSTEM_PROMPT = (
    "You are an independent evidence judge for PostgreSQL knob constraints. "
    "Evaluate only whether the tuple is supported by the provided snippet. "
    "Return ONLY one JSON object with fields: "
    "{"
    "\"support_score\": number between 0 and 1, "
    "\"decision\": \"supported\" | \"uncertain\" | \"unsupported\", "
    "\"canonical_key\": string, "
    "\"reason\": string"
    "}. "
    "Score meaning: 1 = fully supported, 0 = contradicted or unsupported."
)

DEFAULT_JUDGE_ACCEPT_THRESHOLD = 0.60
DEFAULT_JUDGE_UNCERTAINTY_LOW = 0.45
DEFAULT_JUDGE_UNCERTAINTY_HIGH = 0.60
DEFAULT_MIN_CONFIDENCE = 0.55
DEFAULT_IMPUTED_JUDGE_SCORE = 0.70
DEFAULT_IMPUTED_REFLECTION_SCORE = 0.70


KNOB_HEADER_RE = re.compile(r"^([a-z0-9_.]+) \(([^)]+)\)\s*$", re.IGNORECASE)
TOC_LINK_RE = re.compile(r'<span class="sect1"><a href="([^"#]+)"', re.IGNORECASE)
KNOB_ENTRY_RE = re.compile(
    r'<dt[^>]*id="GUC-[^"]*"[^>]*>\s*<span class="term">\s*'
    r'<code class="varname">(?P<knob>[a-z0-9_.]+)</code>\s*'
    r'\(<code class="type">(?P<type>[^<]+)</code>\)',
    re.IGNORECASE | re.DOTALL,
)

RELATION_PRIORITY = {
    "fallback to": 11,
    "defaults proportional to": 10,
    "used in calculation": 10,
    "multiplied by": 9,
    "requires larger": 9,
    "same or higher": 9,
    "smaller than or equal to": 9,
    "less than or equal to half": 9,
    "less than": 8,
    "greater than or equal to": 8,
    "bounded by": 8,
    "subset of": 8,
    "defaults to": 8,
    "requires enabled": 7,
    "requires": 7,
    "interacts with": 6,
    "works with": 6,
    "consider adjusting": 5,
}

RELATION_SIGNAL_TERMS = {
    "same or higher": ["same or higher", "standby server"],
    "less than": ["must be less than", "less than"],
    "greater than or equal to": ["at least as large", "or more may trigger", "greater than or equal"],
    "smaller than or equal to": ["higher than", "no effect", "smaller than or equal"],
    "less than or equal to half": ["half the value", "less than or equal to half"],
    "bounded by": ["limited by", "no effect"],
    "subset of": ["taken from the pool defined by", "subset"],
    "requires": ["must also be enabled", "must be set to replica or higher", "requires"],
    "requires enabled": ["ignored unless", "has no effect unless", "requires enabled"],
    "defaults to": ["if -1 is specified", "value will be used"],
    "fallback to": ["used instead", "fallback"],
    "defaults proportional to": ["1/32", "proportional", "about 3%"],
    "multiplied by": ["multiplying", "multiplied by"],
    "used in calculation": ["tracks locks on", "used in calculation", "computed by"],
    "interacts with": ["relative to", "interacts with"],
    "works with": ["works with", "unarchived data"],
    "consider adjusting": ["consider also adjusting", "consider adjusting"],
    "requires larger": ["require a corresponding increase"],
}


def read_text(path):
    """Read UTF-8 text from disk."""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_text(path, content):
    """Write UTF-8 text to disk."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def load_json(path, default):
    """Load JSON from disk, returning default on missing/invalid file."""
    if not path or not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return default


def save_json(path, payload):
    """Save JSON to disk with stable pretty formatting."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def hash_text(text):
    """Stable hash for dedupe/caching."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def split_paragraphs(text):
    """Group non-empty lines into paragraphs separated by blank lines."""
    parts = []
    buf = []
    for line in text.splitlines():
        if not line.strip():
            if buf:
                parts.append(" ".join(buf).strip())
                buf = []
            continue
        buf.append(line.strip())
    if buf:
        parts.append(" ".join(buf).strip())
    return parts


def chunk_text(text, max_chars=1400):
    """Split text into sentence-based chunks under max_chars."""
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]

    sentences = re.split(r"(?<=[.!?])\s+", text)
    chunks = []
    buf = []
    total = 0
    for sent in sentences:
        if not sent:
            continue
        if total + len(sent) + 1 > max_chars and buf:
            chunks.append(" ".join(buf).strip())
            buf = [sent]
            total = len(sent)
        else:
            buf.append(sent)
            total += len(sent) + 1
    if buf:
        chunks.append(" ".join(buf).strip())
    return chunks


def fetch_url_text(url, timeout, user_agent):
    """Fetch a URL and decode content as text."""
    headers = {"User-Agent": user_agent}
    req = urllib.request.Request(url=url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return body.decode(charset, errors="replace")
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed to fetch {url}: {exc}") from exc


def parse_runtime_config_links(root_html, base_url):
    """Extract section links from chapter TOC and return absolute URLs."""
    urls = []
    seen = set()
    for href in TOC_LINK_RE.findall(root_html):
        abs_url = urllib.parse.urljoin(base_url, href)
        if abs_url in seen:
            continue
        seen.add(abs_url)
        urls.append(abs_url)
    return urls


def html_fragment_to_text(fragment):
    """Convert an HTML fragment to plain text with lightweight block handling."""
    text = fragment
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p>", "\n\n", text)
    text = re.sub(r"(?i)</li>", "\n", text)
    text = re.sub(r"(?i)</(dt|dd|div|tr|table|h1|h2|h3|h4|h5|h6)>", "\n", text)
    text = re.sub(r"(?i)<li[^>]*>", "- ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_knob_entries_from_html(html_text):
    """Extract knob entries as {knob, type, paragraphs} from a doc page."""
    matches = list(KNOB_ENTRY_RE.finditer(html_text))
    entries = []
    for idx, match in enumerate(matches):
        knob = match.group("knob").strip()
        knob_type = html.unescape(match.group("type").strip())

        section_start = match.end()
        section_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(html_text)
        segment = html_text[section_start:section_end]

        dd_open = re.search(r"<dd\b[^>]*>", segment, flags=re.IGNORECASE)
        if not dd_open:
            continue

        # Extract exactly one <dd> block, with nested <dd> support.
        dd_tag_re = re.compile(r"(?i)</?dd\b[^>]*>")
        depth = 1
        dd_end = None
        for tag in dd_tag_re.finditer(segment, dd_open.end()):
            tag_text = tag.group(0).lower()
            if tag_text.startswith("</dd"):
                depth -= 1
                if depth == 0:
                    dd_end = tag.start()
                    break
            else:
                depth += 1

        dd_body = segment[dd_open.end() : dd_end] if dd_end is not None else segment[dd_open.end() :]
        text = html_fragment_to_text(dd_body)
        paragraphs = split_paragraphs(text)
        if not paragraphs:
            continue
        entries.append(
            {
                "knob": knob,
                "type": knob_type,
                "paragraphs": paragraphs,
            }
        )
    return entries


def crawl_runtime_config_docs(start_url, timeout, user_agent, delay, max_pages):
    """Crawl runtime-config chapter + section pages and extract knob entries."""
    root_html = fetch_url_text(start_url, timeout=timeout, user_agent=user_agent)
    toc_urls = parse_runtime_config_links(root_html, start_url)
    page_urls = [start_url] + toc_urls

    unique_urls = []
    seen = set()
    for url in page_urls:
        if url in seen:
            continue
        seen.add(url)
        unique_urls.append(url)
    if max_pages and max_pages > 0:
        unique_urls = unique_urls[:max_pages]

    pages = []
    for idx, url in enumerate(unique_urls):
        if idx == 0:
            html_text = root_html
        else:
            html_text = fetch_url_text(url, timeout=timeout, user_agent=user_agent)

        pages.append(
            {
                "url": url,
                "entries": extract_knob_entries_from_html(html_text),
            }
        )
        if delay > 0 and idx + 1 < len(unique_urls):
            time.sleep(delay)
    return pages


def write_corpus_from_pages(pages, out_path):
    """Write crawled knob content into a line-oriented text corpus."""
    lines = []
    separator = "=" * 80
    for page in pages:
        lines.append(f"URL: {page['url']}")
        lines.append("")
        for entry in page.get("entries", []):
            lines.append(f"{entry['knob']} ({entry['type']})")
            for para in entry.get("paragraphs", []):
                lines.append(para)
                lines.append("")
        lines.append(separator)
        lines.append("")
    write_text(out_path, "\n".join(lines).strip() + "\n")


def extract_sections(text):
    """Locate knob header lines and return section spans plus full line list."""
    lines = text.splitlines()
    sections = []
    current = None
    for i, line in enumerate(lines):
        m = KNOB_HEADER_RE.match(line.strip())
        if not m:
            continue
        knob = m.group(1).strip()
        knob_type = m.group(2).strip()
        if current:
            current["end"] = i
            sections.append(current)
        current = {
            "knob": knob,
            "type": knob_type,
            "start": i + 1,
            "end": len(lines),
        }
    if current:
        current["end"] = len(lines)
        sections.append(current)
    return sections, lines


def load_entries_from_corpus_text(text):
    """Convert section-based corpus text into structured entries."""
    sections, lines = extract_sections(text)
    entries = []
    for sec in sections:
        sec_text = "\n".join(lines[sec["start"] : sec["end"]])
        paragraphs = split_paragraphs(sec_text)
        if not paragraphs:
            continue
        entries.append(
            {
                "knob": sec["knob"],
                "type": sec["type"],
                "paragraphs": paragraphs,
            }
        )
    return entries


def load_knobs_from_relation_json(path):
    """Load unique knob names from relation JSON (for eval or optional knob source)."""
    data = load_json(path, [])
    knobs = set()
    if not isinstance(data, list):
        return []
    for item in data:
        if not isinstance(item, dict):
            continue
        knob1 = str(item.get("knob1") or "").strip()
        knob2 = str(item.get("knob2") or "").strip()
        if knob1:
            knobs.add(knob1)
        if knob2:
            knobs.add(knob2)
    return sorted(knobs)


def load_knobs_from_file(path):
    """Load knob names from a JSON file or a newline-separated text file."""
    if not path:
        return []
    if not os.path.exists(path):
        return []

    if path.lower().endswith(".json"):
        data = load_json(path, [])
        knobs = set()
        if isinstance(data, list):
            for item in data:
                if isinstance(item, str):
                    val = item.strip()
                    if val:
                        knobs.add(val)
                elif isinstance(item, dict):
                    for key in ("knob", "name", "knob1", "knob2"):
                        val = str(item.get(key) or "").strip()
                        if val:
                            knobs.add(val)
        return sorted(knobs)

    knobs = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            knobs.add(line)
    return sorted(knobs)


def build_knob_regex(knobs):
    """Build a case-insensitive regex that matches knob names as whole tokens."""
    knobs_sorted = sorted(set(knobs), key=len, reverse=True)
    escaped = [re.escape(k) for k in knobs_sorted if k]
    if not escaped:
        return None
    pattern = r"(?i)(?<![A-Za-z0-9_.])(" + "|".join(escaped) + r")(?![A-Za-z0-9_.])"
    return re.compile(pattern)


def extend_with_standby_aliases(knobs):
    """Add standby.<knob> aliases for easier normalization of standby mirror rules."""
    expanded = set(knobs)
    for knob in list(expanded):
        if knob.startswith("standby."):
            continue
        expanded.add(f"standby.{knob}")
    return sorted(expanded)


def has_trigger_term(text):
    """Return True when text likely contains dependency wording."""
    lower_text = text.lower()
    return any(term in lower_text for term in TRIGGER_TERMS)


def find_knob_mentions(text, knob_re, lower_to_canonical):
    """Find canonical knob mentions in text while preserving first-seen order."""
    if not knob_re:
        return []
    found = []
    seen = set()
    for mention in knob_re.findall(text):
        canonical = lower_to_canonical.get(mention.lower(), mention)
        if canonical in seen:
            continue
        seen.add(canonical)
        found.append(canonical)
    return found


def paragraph_candidates_for_entry(entry, knob_re, lower_to_canonical, context_window):
    """Select likely-constraint snippets for one knob entry."""
    primary_knob = entry["knob"]
    primary_lower = primary_knob.lower()
    paragraphs = entry.get("paragraphs") or []
    candidates = []

    for idx, para in enumerate(paragraphs):
        para = para.strip()
        if not para:
            continue
        para_lower = para.lower()
        mentions = find_knob_mentions(para, knob_re, lower_to_canonical)
        mention_lowers = {m.lower() for m in mentions}
        other_mentions = [m for m in mentions if m.lower() != primary_lower]

        has_trigger = has_trigger_term(para)
        standby_hint = "standby server" in para_lower or "on the standby" in para_lower

        keep = False
        left = max(0, idx - context_window)
        right = min(len(paragraphs), idx + context_window + 1)
        neighbor_combo = " ".join(paragraphs[left:right]).strip()
        neighbor_mentions = find_knob_mentions(neighbor_combo, knob_re, lower_to_canonical)
        neighbor_mention_lowers = {m.lower() for m in neighbor_mentions}

        if len(mention_lowers) >= 2:
            keep = True
        elif has_trigger and len(other_mentions) >= 1:
            keep = True
        elif has_trigger and standby_hint:
            keep = True
        elif primary_lower in mention_lowers and len(neighbor_mention_lowers) >= 2:
            keep = True
        elif primary_knob in {"bgwriter_delay", "bgwriter_lru_maxpages"} and (
            "background writer" in para_lower or "in each round" in para_lower
        ):
            keep = True

        if not keep:
            continue

        snippet = neighbor_combo
        if not snippet:
            continue

        snippet_mentions = find_knob_mentions(snippet, knob_re, lower_to_canonical)
        if primary_knob not in snippet_mentions:
            snippet_mentions = [primary_knob] + snippet_mentions

        candidates.append(
            {
                "primary_knob": primary_knob,
                "text": snippet,
                "mentions": snippet_mentions,
            }
        )
    return candidates


def build_candidate_chunks(entries, knob_re, lower_to_canonical, context_window, max_chars):
    """Build deduped candidate chunks for LLM extraction."""
    candidates = []
    seen = set()

    for entry in entries:
        para_candidates = paragraph_candidates_for_entry(
            entry=entry,
            knob_re=knob_re,
            lower_to_canonical=lower_to_canonical,
            context_window=context_window,
        )
        for cand in para_candidates:
            for chunk in chunk_text(cand["text"], max_chars=max_chars):
                chunk_id = hash_text(cand["primary_knob"] + "\n" + chunk)
                if chunk_id in seen:
                    continue
                seen.add(chunk_id)
                candidates.append(
                    {
                        "id": chunk_id,
                        "primary_knob": cand["primary_knob"],
                        "mentions": cand["mentions"],
                        "text": chunk,
                    }
                )
    return candidates


def build_prompt(text, primary_knob, knob_scope):
    """Create the user prompt with tight output rules."""
    scope = sorted(set(k for k in knob_scope if k))
    if primary_knob and primary_knob not in scope:
        scope.insert(0, primary_knob)
    rels_str = ", ".join(RELATION_LABELS)
    knobs_str = ", ".join(scope)

    return (
        "Task: Extract explicit knob dependency constraints from the snippet.\n"
        "Rules:\n"
        "- Use only snippet evidence. No outside knowledge.\n"
        "- Output only a JSON array.\n"
        "- relation must be one of allowed labels.\n"
        "- knob1 and knob2 must come from the knob scope list.\n"
        "- If no valid dependency exists, return [].\n"
        "- For standby mirror statements, use knob2 = standby.<primary_knob>.\n"
        "- evidence_span must be a verbatim quote from the snippet.\n"
        f"Primary knob: {primary_knob}\n"
        f"Knob scope: [{knobs_str}]\n"
        f"Allowed relation labels: [{rels_str}]\n"
        "Snippet:\n"
        f"{text}\n"
    )


def resolve_chat_endpoint(api_url):
    """Resolve a chat-completions endpoint from a base URL."""
    url = api_url.rstrip("/")
    if url.endswith("/chat/completions"):
        return url
    return url + "/chat/completions"


def call_chat_completion(
    api_key,
    api_url,
    model,
    system_prompt,
    user_prompt,
    temperature=0.0,
    max_tokens=1024,
    timeout=60,
):
    """Call a chat-completions API endpoint compatible with OpenAI format."""
    endpoint = resolve_chat_endpoint(api_url)
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    req = urllib.request.Request(endpoint, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"API HTTP error {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"API request failed: {exc}") from exc

    parsed = json.loads(raw)
    choices = parsed.get("choices") or []
    if not choices:
        raise RuntimeError(f"API response missing choices: {raw[:500]}")
    message = choices[0].get("message") or {}
    content = message.get("content") or ""
    if isinstance(content, list):
        text_parts = []
        for part in content:
            if isinstance(part, dict) and "text" in part:
                text_parts.append(str(part["text"]))
            else:
                text_parts.append(str(part))
        content = "".join(text_parts)
    return str(content)


def strip_markdown_fences(text):
    """Remove enclosing markdown code fences if present."""
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    if len(lines) >= 2 and lines[-1].strip().startswith("```"):
        return "\n".join(lines[1:-1]).strip()
    return stripped


def extract_json_array(text):
    """Parse a JSON array from model output, tolerating wrapping text/fences."""
    cleaned = strip_markdown_fences((text or "").strip())
    if not cleaned:
        return []
    if cleaned.startswith("["):
        data = json.loads(cleaned)
        return data if isinstance(data, list) else []

    start = cleaned.find("[")
    end = cleaned.rfind("]")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON array found in response")
    candidate = cleaned[start : end + 1]
    data = json.loads(candidate)
    return data if isinstance(data, list) else []


def extract_json_object(text):
    """Parse one JSON object from model output, tolerating wrapping text/fences."""
    cleaned = strip_markdown_fences((text or "").strip())
    if not cleaned:
        return {}
    if cleaned.startswith("{"):
        data = json.loads(cleaned)
        return data if isinstance(data, dict) else {}

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON object found in response")
    candidate = cleaned[start : end + 1]
    data = json.loads(candidate)
    return data if isinstance(data, dict) else {}


def safe_float(value, default=0.0):
    """Best-effort float conversion."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def clamp01(value):
    """Clamp numeric value into [0, 1]."""
    return max(0.0, min(1.0, safe_float(value, 0.0)))


def canonical_condition_text(cond):
    """Normalize condition text into stable key form."""
    if cond is None:
        return ""
    text = str(cond).strip().lower()
    text = re.sub(r"^(if|when)\s+", "", text)
    return text.strip().strip(".")


def build_constraint_key(item):
    """Create stable canonical key for one tuple-like dict."""
    if not isinstance(item, dict):
        return ""
    knob1 = str(item.get("knob1") or "").strip().lower()
    relation = str(item.get("relation") or "").strip().lower()
    knob2 = str(item.get("knob2") or "").strip().lower()
    condition = canonical_condition_text(item.get("condition"))
    return f"{knob1}|{relation}|{knob2}|{condition}"


def build_reflection_prompt(text, primary_knob, knob_scope, items):
    """Build prompt for self-reflection revision."""
    scope = sorted(set(k for k in (knob_scope or []) if k))
    rels_str = ", ".join(RELATION_LABELS)
    knobs_str = ", ".join(scope)
    raw_items = json.dumps(items, ensure_ascii=False, indent=2)
    return (
        "Task: Self-reflect and revise preliminary tuples.\n"
        "Requirements:\n"
        "- Keep only tuples explicitly supported by snippet evidence.\n"
        "- Fix obvious direction reversals.\n"
        "- Keep relation labels inside allowed set.\n"
        "- Keep knobs inside knob scope.\n"
        "- Return ONLY a JSON array with fields: "
        "knob1, relation, knob2, condition(optional), context, evidence_span.\n"
        "- If uncertain, abstain by dropping the tuple.\n"
        f"Primary knob: {primary_knob}\n"
        f"Knob scope: [{knobs_str}]\n"
        f"Allowed relation labels: [{rels_str}]\n"
        "Snippet:\n"
        f"{text}\n\n"
        "Preliminary tuples:\n"
        f"{raw_items}\n"
    )


def build_judge_prompt(text, primary_knob, item):
    """Build prompt for tuple-level independent judge."""
    tuple_json = json.dumps(item, ensure_ascii=False, indent=2)
    return (
        "Task: Judge whether the candidate tuple is supported by the snippet.\n"
        "Guidelines:\n"
        "- Judge evidence support only; do not invent missing links.\n"
        "- Consider directionality and relation semantics.\n"
        "- If support is partial or ambiguous, use decision=uncertain.\n"
        "- canonical_key format: knob1|relation|knob2|condition (all lowercase; empty condition allowed).\n"
        f"Primary knob: {primary_knob}\n"
        "Snippet:\n"
        f"{text}\n\n"
        "Candidate tuple:\n"
        f"{tuple_json}\n"
    )


def call_chat_with_cache(
    cache,
    cache_key,
    cache_path,
    api_key,
    api_url,
    model,
    system_prompt,
    user_prompt,
    timeout,
    temperature=0.0,
    max_tokens=1024,
):
    """Call model with optional response cache."""
    if cache is not None and cache_key and cache_key in cache:
        return str(cache[cache_key])

    response_text = call_chat_completion(
        api_key=api_key,
        api_url=api_url,
        model=model,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )
    if cache is not None and cache_key:
        cache[cache_key] = response_text
        if cache_path:
            save_json(cache_path, cache)
    return response_text


def run_self_reflection(
    items,
    text,
    primary_knob,
    knob_scope,
    api_key,
    api_url,
    model,
    timeout,
    cache,
    cache_path,
    cache_key,
    dry_run,
):
    """Run self-reflection to revise preliminary tuples."""
    out = []
    for item in items:
        if isinstance(item, dict):
            cloned = dict(item)
            cloned["_reflection_score"] = DEFAULT_IMPUTED_REFLECTION_SCORE
            out.append(cloned)
    if not out:
        return []
    if dry_run or not api_key:
        for item in out:
            item["_reflection_score"] = 1.0
        return out

    prompt = build_reflection_prompt(text=text, primary_knob=primary_knob, knob_scope=knob_scope, items=items)
    try:
        response_text = call_chat_with_cache(
            cache=cache,
            cache_key=cache_key,
            cache_path=cache_path,
            api_key=api_key,
            api_url=api_url,
            model=model,
            system_prompt=REFLECTION_SYSTEM_PROMPT,
            user_prompt=prompt,
            timeout=timeout,
            temperature=0.0,
            max_tokens=1200,
        )
        reflected = extract_json_array(response_text)
    except Exception:
        return out

    if not isinstance(reflected, list):
        return out

    # Explicit empty array means abstention for this snippet.
    if not reflected:
        return []

    original_keys = {build_constraint_key(x) for x in items if isinstance(x, dict)}
    revised = []
    for item in reflected:
        if not isinstance(item, dict):
            continue
        cloned = dict(item)
        key = build_constraint_key(cloned)
        cloned["_reflection_score"] = 1.0 if key in original_keys else 0.8
        revised.append(cloned)
    return revised


def judge_constraint_once(
    item,
    text,
    primary_knob,
    api_key,
    api_url,
    model,
    timeout,
    cache,
    cache_path,
    cache_key,
    dry_run,
):
    """Run one independent judge pass for a tuple."""
    fallback_key = build_constraint_key(item)
    if dry_run or not api_key:
        return {
            "score": 1.0,
            "decision": "supported",
            "canonical_key": fallback_key,
            "reason": "dry run",
        }

    prompt = build_judge_prompt(text=text, primary_knob=primary_knob, item=item)
    try:
        response_text = call_chat_with_cache(
            cache=cache,
            cache_key=cache_key,
            cache_path=cache_path,
            api_key=api_key,
            api_url=api_url,
            model=model,
            system_prompt=JUDGE_SYSTEM_PROMPT,
            user_prompt=prompt,
            timeout=timeout,
            temperature=0.0,
            max_tokens=320,
        )
        obj = extract_json_object(response_text)
    except Exception:
        return None

    if not isinstance(obj, dict):
        return None

    score = clamp01(obj.get("support_score"))
    decision = str(obj.get("decision") or "").strip().lower()
    if decision not in {"supported", "uncertain", "unsupported"}:
        if score >= 0.67:
            decision = "supported"
        elif score <= 0.33:
            decision = "unsupported"
        else:
            decision = "uncertain"

    canonical_key = str(obj.get("canonical_key") or "").strip().lower()
    if not canonical_key:
        canonical_key = fallback_key

    reason = str(obj.get("reason") or "").strip()
    return {
        "score": score,
        "decision": decision,
        "canonical_key": canonical_key,
        "reason": reason,
    }


def apply_reliability_guardrail(
    items,
    text,
    primary_knob,
    knob_scope,
    api_key,
    api_url,
    model,
    timeout,
    cache,
    cache_path,
    cache_prefix,
    dry_run,
    enable_reflection,
    enable_judge,
    judge_model,
    judge_api_url,
    judge_accept_threshold,
    judge_uncertainty_low,
    judge_uncertainty_high,
    judge_recheck,
):
    """Apply self-reflection + LLM-as-judge with uncertainty re-check."""
    prelim = [item for item in items if isinstance(item, dict)]
    if not prelim:
        return []

    if enable_reflection:
        reflected = run_self_reflection(
            items=prelim,
            text=text,
            primary_knob=primary_knob,
            knob_scope=knob_scope,
            api_key=api_key,
            api_url=api_url,
            model=model,
            timeout=timeout,
            cache=cache,
            cache_path=cache_path,
            cache_key=f"{cache_prefix}::reflect",
            dry_run=dry_run,
        )
    else:
        reflected = []
        for item in prelim:
            cloned = dict(item)
            cloned["_reflection_score"] = DEFAULT_IMPUTED_REFLECTION_SCORE
            reflected.append(cloned)

    kept = []
    for idx, item in enumerate(reflected):
        if not isinstance(item, dict):
            continue

        tuple_item = dict(item)
        reflection_score = clamp01(tuple_item.get("_reflection_score", DEFAULT_IMPUTED_REFLECTION_SCORE))

        if enable_judge:
            item_key_hash = hash_text(json.dumps(tuple_item, ensure_ascii=False, sort_keys=True))
            j1 = judge_constraint_once(
                item=tuple_item,
                text=text,
                primary_knob=primary_knob,
                api_key=api_key,
                api_url=judge_api_url,
                model=judge_model,
                timeout=timeout,
                cache=cache,
                cache_path=cache_path,
                cache_key=f"{cache_prefix}::judge::{idx}:{item_key_hash}:1",
                dry_run=dry_run,
            )
            if not j1:
                continue

            judge_score = clamp01(j1.get("score", 0.0))
            judge_decision = str(j1.get("decision") or "").strip().lower()
            judge_reason = str(j1.get("reason") or "").strip()
            judge_key = str(j1.get("canonical_key") or "").strip().lower()
            rechecked = False

            if judge_uncertainty_low <= judge_score <= judge_uncertainty_high and judge_recheck:
                rechecked = True
                j2 = judge_constraint_once(
                    item=tuple_item,
                    text=text,
                    primary_knob=primary_knob,
                    api_key=api_key,
                    api_url=judge_api_url,
                    model=judge_model,
                    timeout=timeout,
                    cache=cache,
                    cache_path=cache_path,
                    cache_key=f"{cache_prefix}::judge::{idx}:{item_key_hash}:2",
                    dry_run=dry_run,
                )
                if not j2:
                    continue

                judge_key_2 = str(j2.get("canonical_key") or "").strip().lower()
                if judge_key and judge_key_2 and judge_key != judge_key_2:
                    # Uncertain and unstable => abstain.
                    continue
                judge_score = (judge_score + clamp01(j2.get("score", 0.0))) / 2.0
                if str(j2.get("decision") or "").strip().lower() == "unsupported":
                    judge_decision = "unsupported"
                elif judge_decision != str(j2.get("decision") or "").strip().lower():
                    judge_decision = "uncertain"

            if judge_decision == "unsupported":
                continue
            if judge_score < judge_accept_threshold:
                continue
        else:
            judge_score = DEFAULT_IMPUTED_JUDGE_SCORE
            judge_reason = "judge disabled"
            judge_decision = "supported"
            rechecked = False

        tuple_item["_reflection_score"] = reflection_score
        tuple_item["_judge_score"] = judge_score
        tuple_item["_judge_decision"] = judge_decision
        tuple_item["_judge_rechecked"] = rechecked
        tuple_item["_judge_reason"] = judge_reason
        kept.append(tuple_item)
    return kept


def compute_final_confidence(item):
    """Aggregate judge/reflection/rule support into one confidence score."""
    source = str(item.get("_source") or "llm")
    judge_score = item.get("_judge_score")
    reflection_score = item.get("_reflection_score")
    rule_support = item.get("_rule_support")

    if judge_score is None:
        judge_score = 1.0 if source == "rule" else DEFAULT_IMPUTED_JUDGE_SCORE
    if reflection_score is None:
        reflection_score = 1.0 if source == "rule" else DEFAULT_IMPUTED_REFLECTION_SCORE
    if rule_support is None:
        rule_support = 1.0 if source == "rule" else 0.0

    confidence = (
        0.50 * clamp01(judge_score)
        + 0.30 * clamp01(reflection_score)
        + 0.20 * clamp01(rule_support)
    )
    return round(clamp01(confidence), 4)


def apply_confidence_filter(items, min_confidence):
    """Assign final confidence and drop low-confidence tuples."""
    kept = []
    for item in items:
        if not isinstance(item, dict):
            continue
        cloned = dict(item)
        conf = compute_final_confidence(cloned)
        cloned["_confidence"] = conf
        cloned["confidence"] = conf
        if conf < min_confidence:
            continue
        kept.append(cloned)
    return kept


def normalize_relation(rel):
    """Map relation text to canonical label set."""
    if not rel:
        return None
    rel_clean = str(rel).strip().lower()
    if rel_clean in RELATION_LABELS:
        return rel_clean
    for key, val in RELATION_SYNONYMS.items():
        if rel_clean == key or key in rel_clean:
            return val
    return None


def normalize_knob(name, knob_set, lower_map):
    """Normalize knob names to canonical casing and support standby aliases."""
    if not name:
        return None
    name_clean = str(name).strip().strip("`").strip().rstrip(".")
    if not name_clean:
        return None
    if name_clean in knob_set:
        return name_clean

    lowered = name_clean.lower()
    if lowered in lower_map:
        return lower_map[lowered]

    if lowered.startswith("standby."):
        suffix = lowered.split("standby.", 1)[1]
        if suffix in lower_map:
            return "standby." + lower_map[suffix]
    return None


def infer_standby_peer(knob1, knob2, relation, condition, context, evidence, knob_set):
    """Infer standby.<knob> peer for standby mirror statements when needed."""
    if relation != "same or higher":
        return knob2
    probe = " ".join(
        [
            str(condition or ""),
            str(context or ""),
            str(evidence or ""),
        ]
    ).lower()
    if "standby" not in probe:
        return knob2

    standby_name = f"standby.{knob1}"
    if standby_name not in knob_set:
        return knob2
    if not knob2:
        return standby_name
    if knob2 == knob1:
        return standby_name
    return knob2


def sanitize_extracted_condition(condition, knob1, relation, text_blob):
    """Keep only informative machine-comparable conditions."""
    if condition is None:
        return None
    cond = str(condition).strip()
    if not cond:
        return None
    cond_low = re.sub(r"^(if|when)\s+", "", cond.lower()).strip().strip(".")

    if (
        knob1 == "max_prepared_transactions"
        and relation == "greater than or equal to"
        and ("prepared transactions" in text_blob or "nonzero" in text_blob or "!= 0" in text_blob)
    ):
        return "max_prepared_transactions != 0"
    if "max_prepared_transactions" in cond_low and ("nonzero" in cond_low or "0" in cond_low):
        return "max_prepared_transactions != 0"
    if re.search(r"[a-z0-9_.]+\s*(?:!=|==|<=|>=|<|>)\s*[-a-z0-9_.]+", cond_low):
        return cond_low
    return None


def build_text_blob(item, snippet_text=""):
    """Build a lowercase text blob for heuristics."""
    parts = [
        item.get("evidence_span") or "",
        item.get("context") or "",
        item.get("condition") or "",
        snippet_text or "",
    ]
    return " ".join(str(p) for p in parts if p).lower()


def remap_relation_by_evidence(relation, text_blob):
    """Correct common relation-label mistakes based on evidence wording."""
    rel = relation

    if "same or higher" in text_blob:
        rel = "same or higher"
    if "half the value of" in text_blob:
        rel = "less than or equal to half"
    if "95% of" in text_blob and "freeze" in text_blob:
        rel = "less than"
    if ("at least as large as" in text_blob or "at least as big as" in text_blob) and rel != "same or higher":
        rel = "greater than or equal to"
    if "geqo_threshold or more" in text_blob or "or more may trigger use of the geqo planner" in text_blob:
        rel = "greater than or equal to"
    if "relative to seq_page_cost" in text_blob:
        rel = "interacts with"
    if ("used instead" in text_blob) and ("-1" in text_blob):
        rel = "fallback to"
    elif "-1 is specified" in text_blob and "value will be used" in text_blob:
        rel = "defaults to"
    if "1/32" in text_blob and "shared_buffers" in text_blob:
        rel = "defaults proportional to"
    if "track_counts must also be enabled" in text_blob:
        rel = "requires"
    elif "ignored unless" in text_blob or "has no effect unless" in text_blob:
        rel = "requires enabled"
    elif "must be set to replica or higher" in text_blob:
        rel = "requires"
    if "limited by" in text_blob and "no effect" in text_blob:
        if "higher than" in text_blob:
            rel = "smaller than or equal to"
        elif "half" in text_blob:
            rel = "less than or equal to half"
        else:
            rel = "bounded by"
    if "taken from the pool defined by" in text_blob:
        if "logical replication worker" in text_blob or "synchronization worker" in text_blob:
            rel = "subset of"
    if "tracks locks on" in text_blob and "*" in text_blob and rel != "same or higher":
        rel = "used in calculation"
    if re.search(r"multiplying\s+[a-z0-9_.]+\s+by\s+[a-z0-9_.]+", text_blob):
        rel = "multiplied by"
    if (
        "to limit how old unarchived data can be" in text_blob
        and "archive_timeout" in text_blob
        and "archive_command" in text_blob
    ):
        rel = "works with"
    return rel


def reorient_constraint(knob1, knob2, relation, text_blob, primary_knob, knob_set, lower_map):
    """Fix common direction reversals for asymmetric relations."""
    if relation == "same or higher":
        if knob1.startswith("standby.") and not knob2.startswith("standby."):
            knob1, knob2 = knob2, knob1
        if "standby server" in text_blob and not knob2.startswith("standby."):
            standby_name = f"standby.{knob1}"
            if standby_name in knob_set:
                knob2 = standby_name

    if relation == "requires enabled":
        m = re.search(r"([a-z0-9_.]+)\s+has no effect unless\s+([a-z0-9_.]+)", text_blob)
        if m:
            lhs = normalize_knob(m.group(1), knob_set, lower_map)
            rhs = normalize_knob(m.group(2), knob_set, lower_map)
            if lhs and rhs:
                knob1, knob2 = lhs, rhs

    if relation == "requires":
        m = re.search(
            r"([a-z0-9_.]+)\s+must also be enabled for\s+([a-z0-9_.]+)\s+to work",
            text_blob,
        )
        if m:
            dep = normalize_knob(m.group(1), knob_set, lower_map)
            target = normalize_knob(m.group(2), knob_set, lower_map)
            if target and dep:
                knob1, knob2 = target, dep

    if relation == "multiplied by":
        m = re.search(r"multiplying\s+([a-z0-9_.]+)\s+by\s+([a-z0-9_.]+)", text_blob)
        if m:
            lhs = normalize_knob(m.group(1), knob_set, lower_map)
            rhs = normalize_knob(m.group(2), knob_set, lower_map)
            if lhs and rhs:
                knob1, knob2 = lhs, rhs
        elif knob1.endswith("_multiplier") and knob2 == "work_mem":
            knob1, knob2 = knob2, knob1

    if relation == "used in calculation" and "*" in text_blob:
        star_idx = text_blob.find("*")
        p1 = text_blob.find(knob1.lower())
        p2 = text_blob.find(knob2.lower())
        if p1 != -1 and p2 != -1 and p1 < star_idx < p2:
            knob1, knob2 = knob2, knob1

    if relation == "interacts with" and primary_knob and primary_knob == knob2:
        if "applied after each" in text_blob:
            knob1, knob2 = knob2, knob1

    if relation == "requires enabled":
        if {knob1, knob2} == {"synchronous_commit", "synchronous_standby_names"}:
            knob1, knob2 = "synchronous_standby_names", "synchronous_commit"

    return knob1, knob2


def score_constraint(item, text_blob):
    """Compute a lightweight confidence score for pair-level conflict resolution."""
    relation = item.get("relation") or ""
    score = RELATION_PRIORITY.get(relation, 0)
    signals = RELATION_SIGNAL_TERMS.get(relation, [])
    if any(sig in text_blob for sig in signals):
        score += 3
    else:
        score -= 1

    knob1 = (item.get("knob1") or "").lower()
    knob2 = (item.get("knob2") or "").lower()
    if knob1 and knob1 in text_blob:
        score += 1
    if knob2 and knob2 in text_blob:
        score += 1
    if item.get("condition"):
        score += 1
    return score


def add_rule_constraint(out, seen, knob1, relation, knob2, condition, text_blob, score_boost=5):
    """Append one rule-based constraint if valid and not duplicated."""
    if not knob1 or not knob2 or knob1 == knob2:
        return
    if relation not in RELATION_LABELS:
        return
    key = (knob1, relation, knob2, condition or "")
    if key in seen:
        return
    seen.add(key)
    out.append(
        {
            "knob1": knob1,
            "relation": relation,
            "knob2": knob2,
            "condition": condition,
            "context": "rule-based extraction",
            "evidence_span": text_blob.strip(),
            "_source": "rule",
            "_rule_support": 1.0,
            "_reflection_score": 1.0,
            "_judge_score": 1.0,
            "_score": RELATION_PRIORITY.get(relation, 0) + score_boost,
        }
    )


def extract_rule_constraints_from_text(text, primary_knob, knob_set):
    """Extract high-confidence constraints directly from lexical patterns."""
    out = []
    seen = set()
    lower_map = {k.lower(): k for k in knob_set}
    text_blob = (text or "").strip()
    text_low = text_blob.lower()

    if not text_low or not primary_knob:
        return out

    primary = normalize_knob(primary_knob, knob_set, lower_map)
    if not primary:
        return out

    m = re.search(r"value must be less than\s+([a-z0-9_.]+)", text_low)
    if m:
        dep = normalize_knob(m.group(1), knob_set, lower_map)
        add_rule_constraint(out, seen, primary, "less than", dep, None, text_blob)

    if (
        "same or higher" in text_low
        and "standby server" in text_low
        and primary in {
            "max_connections",
            "max_prepared_transactions",
            "max_worker_processes",
            "max_locks_per_transaction",
        }
    ):
        standby_name = f"standby.{primary}"
        if standby_name in knob_set:
            add_rule_constraint(out, seen, primary, "same or higher", standby_name, None, text_blob)

    if "require a corresponding increase in" in text_low:
        m = re.search(r"corresponding increase in\s+([a-z0-9_.]+)", text_low)
        dep = normalize_knob(m.group(1), knob_set, lower_map) if m else None
        add_rule_constraint(out, seen, primary, "requires larger", dep, None, text_blob)

    m = re.search(r"at least as large as\s+([a-z0-9_.]+)", text_low)
    if m:
        dep = normalize_knob(m.group(1), knob_set, lower_map)
        condition = "max_prepared_transactions != 0" if primary == "max_prepared_transactions" else None
        add_rule_constraint(out, seen, primary, "greater than or equal to", dep, condition, text_blob)

    m = re.search(r"([a-z0-9_.]+)\s*\*\s*\(([^)]+)\)", text_low)
    if m:
        outer = normalize_knob(m.group(1), knob_set, lower_map)
        inner = m.group(2)
        inner_names = re.findall(r"[a-z0-9_.]+", inner)
        for raw in inner_names:
            dep = normalize_knob(raw, knob_set, lower_map)
            add_rule_constraint(out, seen, dep, "used in calculation", outer, None, text_blob)

    m = re.search(r"multiplying\s+([a-z0-9_.]+)\s+by\s+([a-z0-9_.]+)", text_low)
    if m:
        lhs = normalize_knob(m.group(1), knob_set, lower_map)
        rhs = normalize_knob(m.group(2), knob_set, lower_map)
        add_rule_constraint(out, seen, lhs, "multiplied by", rhs, None, text_blob)

    if "-1" in text_low and "used instead" in text_low:
        m = re.search(r"value of\s+([a-z0-9_.]+)\s+should be used instead", text_low)
        dep = normalize_knob(m.group(1), knob_set, lower_map) if m else None
        add_rule_constraint(out, seen, primary, "fallback to", dep, None, text_blob)

    if "-1 is specified" in text_low and "value will be used" in text_low:
        m = re.search(r"regular\s+([a-z0-9_.]+)\s+value will be used", text_low)
        dep = normalize_knob(m.group(1), knob_set, lower_map) if m else None
        add_rule_constraint(out, seen, primary, "defaults to", dep, None, text_blob)

    if "1/32" in text_low and "shared_buffers" in text_low and primary == "wal_buffers":
        dep = normalize_knob("shared_buffers", knob_set, lower_map)
        add_rule_constraint(out, seen, primary, "defaults proportional to", dep, None, text_blob)

    m = re.search(r"limited by\s+([a-z0-9_.]+)", text_low)
    if m:
        dep = normalize_knob(m.group(1), knob_set, lower_map)
        add_rule_constraint(out, seen, primary, "bounded by", dep, None, text_blob)

    if primary == "max_parallel_workers_per_gather" and "taken from the pool" in text_low:
        dep = normalize_knob("max_worker_processes", knob_set, lower_map)
        add_rule_constraint(out, seen, primary, "bounded by", dep, None, text_blob)

    m = re.search(r"taken from the pool defined by\s+([a-z0-9_.]+)", text_low)
    if m:
        dep = normalize_knob(m.group(1), knob_set, lower_map)
        add_rule_constraint(out, seen, primary, "subset of", dep, None, text_blob)

    m = re.search(r"([a-z0-9_.]+)\s+has no effect unless\s+([a-z0-9_.]+)", text_low)
    if m:
        lhs = normalize_knob(m.group(1), knob_set, lower_map)
        rhs = normalize_knob(m.group(2), knob_set, lower_map)
        add_rule_constraint(out, seen, lhs, "requires enabled", rhs, None, text_blob)

    m = re.search(r"([a-z0-9_.]+)\s+must also be enabled for\s+([a-z0-9_.]+)\s+to work", text_low)
    if m:
        dep = normalize_knob(m.group(1), knob_set, lower_map)
        target = normalize_knob(m.group(2), knob_set, lower_map)
        add_rule_constraint(out, seen, target, "requires", dep, None, text_blob)

    if (
        "wal_level must be set to replica or higher" in text_low
        and primary in {"max_wal_senders", "max_replication_slots"}
    ):
        dep = normalize_knob("wal_level", knob_set, lower_map)
        add_rule_constraint(out, seen, primary, "requires", dep, None, text_blob)

    if "geqo_threshold or more" in text_low:
        dep = normalize_knob("geqo_threshold", knob_set, lower_map)
        add_rule_constraint(out, seen, primary, "greater than or equal to", dep, None, text_blob)

    if (
        primary == "from_collapse_limit"
        and "geqo_threshold" in text_low
        and ("or more may trigger" in text_low or "trigger use of the geqo planner" in text_low)
    ):
        dep = normalize_knob("geqo_threshold", knob_set, lower_map)
        add_rule_constraint(out, seen, primary, "greater than or equal to", dep, None, text_blob)

    m = re.search(r"95% of\s+([a-z0-9_.]+)", text_low)
    if m and "freeze" in text_low:
        dep = normalize_knob(m.group(1), knob_set, lower_map)
        add_rule_constraint(out, seen, primary, "less than", dep, None, text_blob)

    m = re.search(r"half the value of\s+([a-z0-9_.]+)", text_low)
    if m:
        dep = normalize_knob(m.group(1), knob_set, lower_map)
        add_rule_constraint(out, seen, primary, "less than or equal to half", dep, None, text_blob)

    if (
        primary == "random_page_cost"
        and "seq_page_cost" in text_low
        and (
            "relative to seq_page_cost" in text_low
            or "prefer index scans" in text_low
            or "look relatively more expensive" in text_low
        )
    ):
        dep = normalize_knob("seq_page_cost", knob_set, lower_map)
        add_rule_constraint(out, seen, primary, "interacts with", dep, None, text_blob)

    if (
        primary in {"bgwriter_lru_maxpages", "bgwriter_delay"}
        and "bgwriter_delay" in text_low
        and "bgwriter_lru_maxpages" in text_low
        and "applied after each" in text_low
    ):
        dep = normalize_knob("bgwriter_delay", knob_set, lower_map)
        other = normalize_knob("bgwriter_lru_maxpages", knob_set, lower_map)
        add_rule_constraint(out, seen, dep, "interacts with", other, None, text_blob)

    if (
        primary == "bgwriter_lru_maxpages"
        and "background writer" in text_low
        and "in each round" in text_low
    ):
        dep = normalize_knob("bgwriter_delay", knob_set, lower_map)
        add_rule_constraint(out, seen, dep, "interacts with", primary, None, text_blob)

    if (
        primary == "bgwriter_delay"
        and "in each round" in text_low
        and "controllable by the following parameters" in text_low
    ):
        other = normalize_knob("bgwriter_lru_maxpages", knob_set, lower_map)
        add_rule_constraint(out, seen, primary, "interacts with", other, None, text_blob)

    if primary == "archive_timeout" and "archive_command" in text_low and "unarchived data" in text_low:
        dep = normalize_knob("archive_command", knob_set, lower_map)
        add_rule_constraint(out, seen, primary, "works with", dep, None, text_blob)

    if primary == "max_parallel_workers" and "higher than" in text_low and "no effect" in text_low:
        dep = normalize_knob("max_worker_processes", knob_set, lower_map)
        add_rule_constraint(out, seen, primary, "smaller than or equal to", dep, None, text_blob)

    if primary == "max_worker_processes" and "consider also adjusting" in text_low:
        for dep_name in [
            "max_parallel_workers",
            "max_parallel_maintenance_workers",
            "max_parallel_workers_per_gather",
        ]:
            dep = normalize_knob(dep_name, knob_set, lower_map)
            add_rule_constraint(out, seen, primary, "consider adjusting", dep, None, text_blob)

    return out


def normalize_constraints(items, knob_set, primary_knob=None, snippet_text="", anchor_primary=True):
    """Filter and normalize model outputs into canonical schema."""
    normalized = []
    lower_map = {k.lower(): k for k in knob_set}
    for item in items:
        if not isinstance(item, dict):
            continue

        relation = normalize_relation(item.get("relation"))
        if not relation:
            continue

        knob1 = normalize_knob(item.get("knob1"), knob_set, lower_map)
        knob2 = normalize_knob(item.get("knob2"), knob_set, lower_map)
        context = (item.get("context") or "").strip()
        evidence = (item.get("evidence_span") or "").strip()

        if not knob1 or not knob2:
            continue

        knob2 = infer_standby_peer(
            knob1,
            knob2,
            relation,
            item.get("condition"),
            context,
            evidence,
            knob_set,
        )
        text_blob = build_text_blob(item, snippet_text=snippet_text)
        relation = remap_relation_by_evidence(relation, text_blob)
        if relation not in RELATION_LABELS:
            continue

        knob1, knob2 = reorient_constraint(
            knob1,
            knob2,
            relation,
            text_blob=text_blob,
            primary_knob=primary_knob,
            knob_set=knob_set,
            lower_map=lower_map,
        )
        if not knob1 or not knob2 or knob1 == knob2:
            continue

        condition = sanitize_extracted_condition(item.get("condition"), knob1, relation, text_blob)
        source = str(item.get("_source") or "llm").strip().lower() or "llm"
        reflection_score = item.get("_reflection_score")
        judge_score = item.get("_judge_score")
        judge_decision = item.get("_judge_decision")
        judge_rechecked = item.get("_judge_rechecked")
        judge_reason = item.get("_judge_reason")
        rule_support = item.get("_rule_support")

        if anchor_primary and primary_knob:
            standby_primary = f"standby.{primary_knob}"
            if not (
                knob1 == primary_knob
                or knob2 == primary_knob
                or knob1 == standby_primary
                or knob2 == standby_primary
                or relation == "used in calculation"
            ):
                continue

        normalized.append(
            {
                "knob1": knob1,
                "relation": relation,
                "knob2": knob2,
                "condition": condition,
                "context": context,
                "evidence_span": evidence,
                "_source": source,
                "_reflection_score": reflection_score,
                "_judge_score": judge_score,
                "_judge_decision": judge_decision,
                "_judge_rechecked": judge_rechecked,
                "_judge_reason": judge_reason,
                "_rule_support": rule_support,
                "_score": score_constraint(
                    {
                        "knob1": knob1,
                        "knob2": knob2,
                        "relation": relation,
                        "condition": condition,
                    },
                    text_blob,
                ),
            }
        )
    return normalized


def dedupe_constraints(items):
    """Drop duplicate constraints by (knob1, relation, knob2, condition)."""
    best_by_key = {}
    key_order = []
    for item in items:
        key = (
            item.get("knob1"),
            item.get("relation"),
            item.get("knob2"),
            item.get("condition") or "",
        )
        current = best_by_key.get(key)
        if current is None:
            best_by_key[key] = item
            key_order.append(key)
            continue
        cur_conf = safe_float(current.get("_confidence"), -1.0)
        new_conf = safe_float(item.get("_confidence"), -1.0)
        cur_score = safe_float(current.get("_score"), 0.0)
        new_score = safe_float(item.get("_score"), 0.0)
        if new_conf > cur_conf or (new_conf == cur_conf and new_score > cur_score):
            best_by_key[key] = item
    return [best_by_key[k] for k in key_order]


def keep_best_per_pair(items):
    """Keep one highest-scoring item for each directed pair (knob1, knob2)."""
    best = {}
    for item in items:
        pair = (item.get("knob1"), item.get("knob2"))
        confidence = safe_float(item.get("_confidence"), -1.0)
        score = safe_float(item.get("_score"), 0.0)
        current = best.get(pair)
        if current is None:
            best[pair] = item
            continue

        current_conf = safe_float(current.get("_confidence"), -1.0)
        current_score = safe_float(current.get("_score"), 0.0)
        if confidence > current_conf:
            best[pair] = item
            continue
        if confidence < current_conf:
            continue
        if score > current_score:
            best[pair] = item
            continue
        if score == current_score and item.get("condition") and not current.get("condition"):
            best[pair] = item
    return list(best.values())


def strip_internal_fields(items):
    """Remove internal bookkeeping keys before writing outputs."""
    cleaned = []
    for item in items:
        out = {}
        for key, value in item.items():
            if key.startswith("_"):
                continue
            out[key] = value
        cleaned.append(out)
    return cleaned


def apply_precision_filters(items):
    """Drop recurring noisy patterns that are outside target dependency semantics."""
    filtered = []
    for item in items:
        knob1 = item.get("knob1") or ""
        knob2 = item.get("knob2") or ""
        relation = item.get("relation") or ""

        if (knob1.startswith("standby.") or knob2.startswith("standby.")) and relation != "same or higher":
            continue
        if relation == "used in calculation" and (knob1.startswith("standby.") or knob2.startswith("standby.")):
            continue
        if relation == "greater than or equal to" and knob1.startswith("standby."):
            continue
        if relation == "consider adjusting" and knob1 != "max_worker_processes":
            continue
        if relation == "consider adjusting" and knob1 == "max_worker_processes" and knob2 == "max_parallel_workers":
            continue
        if relation == "requires" and knob1 == "archive_mode" and knob2 == "wal_level":
            continue
        if relation == "greater than or equal to" and knob1 == "join_collapse_limit" and knob2 == "from_collapse_limit":
            continue
        if relation == "interacts with" and knob1 == "max_parallel_workers_per_gather" and knob2 == "work_mem":
            continue
        if relation == "same or higher" and knob1 == "max_wal_senders" and knob2 == "standby.max_wal_senders":
            continue

        filtered.append(item)
    return filtered


def normalize_condition(cond):
    """Normalize condition text for evaluation."""
    if not cond:
        return ""
    cond_text = str(cond).strip().lower()
    cond_text = re.sub(r"^(if|when)\s+", "", cond_text)
    return cond_text.strip().strip(".")


def load_constraints_for_eval(path):
    """Load constraints JSON and normalize to eval fields."""
    data = load_json(path, [])
    cleaned = []
    invalid = 0
    if not isinstance(data, list):
        return cleaned, 0
    for item in data:
        if not isinstance(item, dict):
            invalid += 1
            continue

        knob1 = str(item.get("knob1") or "").strip()
        knob2 = str(item.get("knob2") or "").strip()
        relation = normalize_relation(item.get("relation") or "")
        if not relation:
            relation = str(item.get("relation") or "").strip().lower()
        if not knob1 or not knob2 or not relation:
            invalid += 1
            continue

        cleaned.append(
            {
                "knob1": knob1,
                "relation": relation,
                "knob2": knob2,
                "condition": normalize_condition(item.get("condition")),
            }
        )
    return cleaned, invalid


def build_eval_key(item, include_condition):
    """Create tuple key for matching predictions to ground truth."""
    if include_condition:
        return (
            item["knob1"],
            item["relation"],
            item["knob2"],
            item.get("condition") or "",
        )
    return (item["knob1"], item["relation"], item["knob2"])


def compute_metrics(pred_items, gt_items, include_condition):
    """Compute precision/recall/F1 from set overlap."""
    pred_keys = {build_eval_key(i, include_condition) for i in pred_items}
    gt_keys = {build_eval_key(i, include_condition) for i in gt_items}
    tp = len(pred_keys & gt_keys)
    fp = len(pred_keys - gt_keys)
    fn = len(gt_keys - pred_keys)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if precision + recall else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }, pred_keys, gt_keys


def eval_per_relation(pred_items, gt_items, include_condition):
    """Compute per-relation metrics."""
    results = {}
    for rel in RELATION_LABELS:
        pred_rel = [i for i in pred_items if i["relation"] == rel]
        gt_rel = [i for i in gt_items if i["relation"] == rel]
        metrics, _, _ = compute_metrics(pred_rel, gt_rel, include_condition)
        metrics["pred_count"] = len(pred_rel)
        metrics["gt_count"] = len(gt_rel)
        results[rel] = metrics
    return results


def format_key(key, include_condition):
    """Format key for human-readable error logs."""
    if include_condition and len(key) == 4 and key[3]:
        return f"{key[0]} {key[1]} {key[2]} [if {key[3]}]"
    return f"{key[0]} {key[1]} {key[2]}"


def run_evaluation(pred_path, gt_path, eval_out, show_errors):
    """Run evaluation and print summary."""
    pred_items, pred_invalid = load_constraints_for_eval(pred_path)
    gt_items, gt_invalid = load_constraints_for_eval(gt_path)

    metrics_cond, pred_keys_cond, gt_keys_cond = compute_metrics(pred_items, gt_items, True)
    metrics_nocond, _, _ = compute_metrics(pred_items, gt_items, False)

    per_rel_cond = eval_per_relation(pred_items, gt_items, True)
    per_rel_nocond = eval_per_relation(pred_items, gt_items, False)

    summary = {
        "pred_total": len(pred_items),
        "gt_total": len(gt_items),
        "pred_invalid": pred_invalid,
        "gt_invalid": gt_invalid,
        "condition_aware": metrics_cond,
        "condition_agnostic": metrics_nocond,
        "per_relation_condition_aware": per_rel_cond,
        "per_relation_condition_agnostic": per_rel_nocond,
    }

    print("Evaluation (condition-aware):")
    print(
        f"  P={metrics_cond['precision']:.3f} R={metrics_cond['recall']:.3f} "
        f"F1={metrics_cond['f1']:.3f} TP={metrics_cond['tp']} "
        f"FP={metrics_cond['fp']} FN={metrics_cond['fn']}"
    )
    print("Evaluation (condition-agnostic):")
    print(
        f"  P={metrics_nocond['precision']:.3f} R={metrics_nocond['recall']:.3f} "
        f"F1={metrics_nocond['f1']:.3f} TP={metrics_nocond['tp']} "
        f"FP={metrics_nocond['fp']} FN={metrics_nocond['fn']}"
    )

    if show_errors and show_errors > 0:
        fp_keys = list(pred_keys_cond - gt_keys_cond)[:show_errors]
        fn_keys = list(gt_keys_cond - pred_keys_cond)[:show_errors]
        print(f"Sample false positives (up to {show_errors}):")
        for key in fp_keys:
            print("  - " + format_key(key, True))
        print(f"Sample false negatives (up to {show_errors}):")
        for key in fn_keys:
            print("  - " + format_key(key, True))
        summary["sample_false_positives"] = [format_key(k, True) for k in fp_keys]
        summary["sample_false_negatives"] = [format_key(k, True) for k in fn_keys]

    if eval_out:
        save_json(eval_out, summary)


def render_progress(current, total, bar_width=30):
    """Render a simple ASCII progress bar."""
    if total <= 0:
        return ""
    ratio = current / total
    filled = int(bar_width * ratio)
    bar = "=" * filled + "-" * (bar_width - filled)
    return f"[{bar}] {current}/{total} ({ratio * 100:.1f}%)"


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Extract PostgreSQL knob constraints from docs.")

    parser.add_argument("--input", default="pg13_all.txt", help="Input corpus text file.")
    parser.add_argument("--out-raw", default="constraints_raw.json", help="Raw extracted constraints.")
    parser.add_argument("--out-normalized", default="constraints_normalized.json", help="Normalized constraints.")
    parser.add_argument("--cache", default="constraints_cache.json", help="LLM response cache.")
    parser.add_argument("--candidates-out", default="", help="Optional JSON dump of candidate chunks.")
    parser.add_argument(
        "--normalize-only",
        action="store_true",
        help="Post-process an existing JSON prediction file without crawling or API calls.",
    )
    parser.add_argument(
        "--normalize-input",
        default="",
        help="Input JSON for --normalize-only (defaults to --out-raw).",
    )

    parser.add_argument("--crawl", action="store_true", help="Crawl PostgreSQL runtime-config docs into --input.")
    parser.add_argument("--crawl-only", action="store_true", help="Only crawl docs and exit.")
    parser.add_argument("--start-url", default=DEFAULT_RUNTIME_CONFIG_URL, help="Runtime-config chapter URL.")
    parser.add_argument("--crawl-timeout", type=int, default=25, help="HTTP timeout for crawling (seconds).")
    parser.add_argument("--crawl-delay", type=float, default=0.2, help="Sleep between page downloads.")
    parser.add_argument("--crawl-max-pages", type=int, default=0, help="Limit crawled pages (0 = all).")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="HTTP user-agent for crawling.")

    parser.add_argument("--context-window", type=int, default=1, help="Neighbor paragraphs to include.")
    parser.add_argument("--max-chars", type=int, default=1400, help="Max characters per candidate chunk.")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of chunks for quick runs.")

    parser.add_argument("--dry-run", action="store_true", help="Do not call LLM API.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Model name.")
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="Base URL or chat-completions endpoint.")
    parser.add_argument("--api-key-env", default="DEEPSEEK_API_KEY", help="Environment variable for API key.")
    parser.add_argument("--api-timeout", type=int, default=60, help="LLM API timeout (seconds).")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep between API calls.")
    parser.add_argument("--progress", action="store_true", help="Show progress bar during extraction.")
    parser.add_argument(
        "--disable-self-reflection",
        action="store_true",
        help="Disable Step-3 self-reflection guardrail.",
    )
    parser.add_argument(
        "--disable-llm-judge",
        action="store_true",
        help="Disable Step-3 LLM-as-Judge guardrail.",
    )
    parser.add_argument("--judge-model", default="", help="Judge model (defaults to --model).")
    parser.add_argument("--judge-api-url", default="", help="Judge API URL (defaults to --api-url).")
    parser.add_argument(
        "--judge-accept-threshold",
        type=float,
        default=DEFAULT_JUDGE_ACCEPT_THRESHOLD,
        help="Minimum judge support score to keep a tuple.",
    )
    parser.add_argument(
        "--judge-uncertainty-low",
        type=float,
        default=DEFAULT_JUDGE_UNCERTAINTY_LOW,
        help="Lower bound of judge uncertainty band.",
    )
    parser.add_argument(
        "--judge-uncertainty-high",
        type=float,
        default=DEFAULT_JUDGE_UNCERTAINTY_HIGH,
        help="Upper bound of judge uncertainty band.",
    )
    parser.add_argument(
        "--disable-judge-recheck",
        action="store_true",
        help="Disable one-time re-check for tuples in judge uncertainty band.",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=DEFAULT_MIN_CONFIDENCE,
        help="Drop tuples below final aggregated confidence.",
    )
    parser.add_argument(
        "--disable-confidence-filter",
        action="store_true",
        help="Keep low-confidence tuples (confidence field is still computed).",
    )

    parser.add_argument("--relation-json", default="relation.json", help="Ground-truth constraints file.")
    parser.add_argument(
        "--knob-source",
        default="docs",
        choices=["docs", "relation", "file"],
        help="Knob vocabulary source. Use 'file' with --knob-file for non-leaky targeted extraction.",
    )
    parser.add_argument(
        "--knob-file",
        default="",
        help="Path to knob list (json or txt) used when --knob-source file.",
    )
    parser.add_argument(
        "--disable-standby-aliases",
        action="store_true",
        help="Do not add synthetic standby.<knob> aliases.",
    )
    parser.add_argument(
        "--restrict-primary-to-knob-source",
        action="store_true",
        help="Only process sections whose primary knob is in the selected knob source list.",
    )
    parser.add_argument(
        "--disable-primary-anchor",
        action="store_true",
        help="Allow predictions not anchored to the chunk primary knob.",
    )
    parser.add_argument(
        "--disable-best-per-pair",
        action="store_true",
        help="Keep all relation variants per pair instead of highest-confidence only.",
    )

    parser.add_argument("--evaluate", action="store_true", help="Evaluate after extraction.")
    parser.add_argument("--eval-only", action="store_true", help="Only run evaluation.")
    parser.add_argument("--eval-pred", default="", help="Prediction JSON file for evaluation.")
    parser.add_argument("--eval-gt", default="relation.json", help="Ground truth JSON file.")
    parser.add_argument("--eval-out", default="", help="Write evaluation summary as JSON.")
    parser.add_argument("--eval-show", type=int, default=0, help="Show sample FP/FN rows.")

    args = parser.parse_args()
    args.judge_model = args.judge_model or args.model
    args.judge_api_url = args.judge_api_url or args.api_url
    args.judge_accept_threshold = clamp01(args.judge_accept_threshold)
    args.judge_uncertainty_low = clamp01(args.judge_uncertainty_low)
    args.judge_uncertainty_high = clamp01(args.judge_uncertainty_high)
    if args.judge_uncertainty_low > args.judge_uncertainty_high:
        args.judge_uncertainty_low, args.judge_uncertainty_high = (
            args.judge_uncertainty_high,
            args.judge_uncertainty_low,
        )
    args.min_confidence = clamp01(args.min_confidence)

    if args.eval_only:
        pred_path = args.eval_pred or args.out_normalized
        run_evaluation(pred_path, args.eval_gt, args.eval_out, args.eval_show)
        return

    if args.normalize_only:
        source_path = args.normalize_input or args.out_raw
        source_items = load_json(source_path, [])
        if not isinstance(source_items, list):
            print(f"normalize-only input is not a JSON array: {source_path}", file=sys.stderr)
            sys.exit(1)

        # Build normalization knob vocabulary.
        if args.knob_source == "relation":
            knob_list = load_knobs_from_relation_json(args.relation_json)
        elif args.knob_source == "file":
            if not args.knob_file:
                print("Missing --knob-file while using --knob-source file.", file=sys.stderr)
                sys.exit(1)
            if not os.path.exists(args.knob_file):
                print(f"Knob file not found: {args.knob_file}", file=sys.stderr)
                sys.exit(1)
            knob_list = load_knobs_from_file(args.knob_file)
        else:
            knob_list = sorted(
                {
                    x
                    for item in source_items
                    if isinstance(item, dict)
                    for x in [item.get("knob1"), item.get("knob2")]
                    if isinstance(x, str) and x.strip()
                }
            )
        if not knob_list:
            print("No knobs available for normalize-only mode.", file=sys.stderr)
            sys.exit(1)

        normalization_knobs = list(knob_list)
        if not args.disable_standby_aliases:
            normalization_knobs = extend_with_standby_aliases(normalization_knobs)
        knob_set = set(normalization_knobs)

        normalized = []
        for item in source_items:
            if not isinstance(item, dict):
                continue
            primary_knob = str(item.get("knob1") or "").strip() or None
            snippet_text = str(item.get("evidence_span") or item.get("context") or "")
            normalized.extend(
                normalize_constraints(
                    [item],
                    knob_set,
                    primary_knob=primary_knob,
                    snippet_text=snippet_text,
                    anchor_primary=not args.disable_primary_anchor,
                )
            )
            normalized.extend(
                extract_rule_constraints_from_text(
                    text=snippet_text,
                    primary_knob=primary_knob,
                    knob_set=knob_set,
                )
            )
        normalized = dedupe_constraints(normalized)
        conf_threshold = 0.0 if args.disable_confidence_filter else args.min_confidence
        normalized = apply_confidence_filter(normalized, min_confidence=conf_threshold)
        if not args.disable_best_per_pair:
            normalized = keep_best_per_pair(normalized)
        normalized = apply_precision_filters(normalized)
        normalized = strip_internal_fields(normalized)
        save_json(args.out_normalized, normalized)

        if args.evaluate:
            pred_path = args.eval_pred or args.out_normalized
            run_evaluation(pred_path, args.eval_gt, args.eval_out, args.eval_show)
        return

    if args.crawl or not os.path.exists(args.input):
        pages = crawl_runtime_config_docs(
            start_url=args.start_url,
            timeout=args.crawl_timeout,
            user_agent=args.user_agent,
            delay=args.crawl_delay,
            max_pages=args.crawl_max_pages,
        )
        total_entries = sum(len(page.get("entries", [])) for page in pages)
        write_corpus_from_pages(pages, args.input)
        print(
            f"Crawled {len(pages)} pages and wrote {total_entries} knob entries to {args.input}.",
            file=sys.stderr,
        )
        if args.crawl_only:
            return

    corpus = read_text(args.input)
    entries = load_entries_from_corpus_text(corpus)
    if not entries:
        print(f"No knob sections found in {args.input}.", file=sys.stderr)
        sys.exit(1)

    doc_knobs = sorted({entry["knob"] for entry in entries if entry.get("knob")})
    if args.knob_source == "relation":
        print(
            "Using --knob-source relation. This can leak ground-truth knob vocabulary.",
            file=sys.stderr,
        )
        knob_list = load_knobs_from_relation_json(args.relation_json)
    elif args.knob_source == "file":
        if not args.knob_file:
            print("Missing --knob-file while using --knob-source file.", file=sys.stderr)
            sys.exit(1)
        if not os.path.exists(args.knob_file):
            print(f"Knob file not found: {args.knob_file}", file=sys.stderr)
            sys.exit(1)
        knob_list = load_knobs_from_file(args.knob_file)
    else:
        knob_list = doc_knobs
    if not knob_list:
        print("Knob list is empty; cannot continue.", file=sys.stderr)
        sys.exit(1)

    normalization_knobs = list(knob_list)
    if not args.disable_standby_aliases:
        normalization_knobs = extend_with_standby_aliases(normalization_knobs)
    knob_set = set(normalization_knobs)

    mention_knobs = doc_knobs
    if args.knob_source != "docs":
        mention_knobs = sorted({k for k in knob_list if not k.startswith("standby.")})
    mention_knob_set = set(mention_knobs)

    if args.restrict_primary_to_knob_source or args.knob_source != "docs":
        entries = [entry for entry in entries if entry.get("knob") in mention_knob_set]

    lower_to_canonical = {k.lower(): k for k in mention_knobs}
    knob_re = build_knob_regex(mention_knobs)

    candidates = build_candidate_chunks(
        entries=entries,
        knob_re=knob_re,
        lower_to_canonical=lower_to_canonical,
        context_window=max(0, args.context_window),
        max_chars=max(200, args.max_chars),
    )
    if args.limit and args.limit > 0:
        candidates = candidates[: args.limit]
    if not candidates:
        print("No candidate snippets detected. Try broader trigger terms or context window.", file=sys.stderr)
        save_json(args.out_raw, [])
        save_json(args.out_normalized, [])
        return
    if args.candidates_out:
        save_json(args.candidates_out, candidates)

    cache = load_json(args.cache, {}) if args.cache else {}
    if not isinstance(cache, dict):
        cache = {}

    api_key = os.environ.get(args.api_key_env, "")
    if not api_key and not args.dry_run:
        print(f"Missing API key in env var {args.api_key_env}.", file=sys.stderr)
        sys.exit(1)

    raw_results = []
    normalized_results = []
    total_chunks = len(candidates)

    for idx, cand in enumerate(candidates, start=1):
        chunk_id = cand["id"]
        knob_scope = list(cand.get("mentions") or [])
        if "standby" in (cand.get("text") or "").lower():
            knob_scope.append(f"standby.{cand['primary_knob']}")

        cache_hit = chunk_id in cache
        if args.dry_run and not cache_hit:
            response_text = "[]"
        else:
            prompt = build_prompt(
                text=cand["text"],
                primary_knob=cand["primary_knob"],
                knob_scope=knob_scope,
            )
            response_text = call_chat_with_cache(
                cache=cache,
                cache_key=chunk_id,
                cache_path=args.cache,
                api_key=api_key,
                api_url=args.api_url,
                model=args.model,
                system_prompt=SYSTEM_PROMPT,
                user_prompt=prompt,
                temperature=0.0,
                max_tokens=1024,
                timeout=args.api_timeout,
            )
            if args.sleep > 0 and not cache_hit:
                time.sleep(args.sleep)

        try:
            items = extract_json_array(response_text)
        except Exception:
            items = []

        raw_results.extend(items)
        guardrailed_items = apply_reliability_guardrail(
            items=items,
            text=cand.get("text") or "",
            primary_knob=cand.get("primary_knob"),
            knob_scope=knob_scope,
            api_key=api_key,
            api_url=args.api_url,
            model=args.model,
            timeout=args.api_timeout,
            cache=cache,
            cache_path=args.cache,
            cache_prefix=chunk_id,
            dry_run=args.dry_run,
            enable_reflection=not args.disable_self_reflection,
            enable_judge=not args.disable_llm_judge,
            judge_model=args.judge_model,
            judge_api_url=args.judge_api_url,
            judge_accept_threshold=args.judge_accept_threshold,
            judge_uncertainty_low=args.judge_uncertainty_low,
            judge_uncertainty_high=args.judge_uncertainty_high,
            judge_recheck=not args.disable_judge_recheck,
        )
        normalized_results.extend(
            normalize_constraints(
                guardrailed_items,
                knob_set,
                primary_knob=cand.get("primary_knob"),
                snippet_text=cand.get("text") or "",
                anchor_primary=not args.disable_primary_anchor,
            )
        )
        normalized_results.extend(
            extract_rule_constraints_from_text(
                text=cand.get("text") or "",
                primary_knob=cand.get("primary_knob"),
                knob_set=knob_set,
            )
        )

        if args.progress and sys.stderr.isatty():
            bar = render_progress(idx, total_chunks)
            sys.stderr.write("\r" + bar)
            sys.stderr.flush()
        elif idx % 25 == 0 or idx == total_chunks:
            print(f"Processed {idx}/{total_chunks} chunks", file=sys.stderr)

    if args.progress and sys.stderr.isatty():
        sys.stderr.write("\n")
        sys.stderr.flush()

    raw_results = dedupe_constraints(raw_results)
    normalized_results = dedupe_constraints(normalized_results)
    conf_threshold = 0.0 if args.disable_confidence_filter else args.min_confidence
    normalized_results = apply_confidence_filter(normalized_results, min_confidence=conf_threshold)
    if not args.disable_best_per_pair:
        normalized_results = keep_best_per_pair(normalized_results)
    normalized_results = apply_precision_filters(normalized_results)
    normalized_results = strip_internal_fields(normalized_results)

    save_json(args.out_raw, raw_results)
    save_json(args.out_normalized, normalized_results)

    if args.evaluate:
        pred_path = args.eval_pred or args.out_normalized
        run_evaluation(pred_path, args.eval_gt, args.eval_out, args.eval_show)


if __name__ == "__main__":
    main()
