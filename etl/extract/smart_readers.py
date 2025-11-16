# etl/extract/smart_readers.py
"""
Robust "smart" readers for messy Tier-B / Tier-C files.

Provides:
- smart_read_parts(path) -> dict of DataFrames by source (json, html, csv, kv, raw_text)
- smart_read_combined(path) -> single DataFrame combined from available parts
- smart_read(path) -> alias to smart_read_combined (backwards compat)
"""
import re
import io
import csv
import json
import logging
from typing import List, Dict, Any
from bs4 import BeautifulSoup
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# JSON extraction helpers
# ---------------------------------------------------------
def _extract_json_blocks(text: str) -> List[str]:
    """
    Heuristic detection of JSON blocks including fenced ```json``` code blocks
    and standalone {...} or [...]. Returns list of JSON text snippets.
    """
    blocks = []
    # fenced json code blocks
    blocks += re.findall(r"```json\s*(\{[\s\S]*?\}|\[[\s\S]*?\])\s*```", text, flags=re.I)
    # standalone {...} or [...] (greedy but filtered)
    blocks += re.findall(r"(\{[\s\S]*?\}|\[[\s\S]*?\])", text, flags=re.S)
    # dedupe and basic filtering
    seen = set()
    out = []
    for b in blocks:
        s = b.strip()
        if s and s not in seen and (s.startswith("{") or s.startswith("[")):
            # skip tiny matches
            if len(s) < 3:
                continue
            seen.add(s)
            out.append(s)
    return out


def parse_json_block(text: str) -> pd.DataFrame:
    try:
        obj = json.loads(text)
        return pd.DataFrame(obj if isinstance(obj, list) else [obj])
    except Exception as e:
        logger.debug("parse_json_block failed: %s", e)
        return pd.DataFrame()


# ---------------------------------------------------------
# HTML table extraction helpers
# ---------------------------------------------------------
def _extract_table_blocks(text: str) -> List[str]:
    # capture literal <table>...</table> blocks
    return re.findall(r"(<table[\s\S]*?</table>)", text, flags=re.I)


def parse_html_table(html_str: str) -> pd.DataFrame:
    """
    Try pandas.read_html first, then fallback to BeautifulSoup manual parsing.
    """
    try:
        tables = pd.read_html(io.StringIO(html_str), flavor="bs4")
        if tables:
            return tables[0]
    except Exception as e:
        logger.debug("pd.read_html failed: %s", e)

    # fallback manual parse with BS4
    try:
        soup = BeautifulSoup(html_str, "html.parser")
        table = soup.find("table")
        if not table:
            return pd.DataFrame()
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        rows = []
        for tr in table.find_all("tr"):
            cols = [td.get_text(strip=True) for td in tr.find_all(["td"])]
            if cols:
                rows.append(cols)
        if headers and rows:
            return pd.DataFrame(rows, columns=headers)
        elif rows:
            return pd.DataFrame(rows)
    except Exception as e:
        logger.debug("BeautifulSoup table parse failed: %s", e)
    return pd.DataFrame()


# ---------------------------------------------------------
# Improved CSV detection & parsing (robust)
# ---------------------------------------------------------
def _is_candidate_csv_block(lines: List[str], min_lines: int = 2) -> bool:
    non_empty = [l for l in lines if l.strip()]
    if len(non_empty) < min_lines:
        return False

    comma_counts = [l.count(",") for l in non_empty]
    tab_counts = [l.count("\t") for l in non_empty]

    if sum(comma_counts) >= sum(tab_counts) and sum(comma_counts) > 0:
        counts = comma_counts
    elif sum(tab_counts) > 0:
        counts = tab_counts
    else:
        return False

    # require that most lines have similar delimiter counts (>=60%)
    most_common = max(set(counts), key=counts.count)
    consistent_ratio = sum(1 for c in counts if c == most_common) / len(counts)
    return consistent_ratio >= 0.6


def _extract_csv_blocks(text: str, min_lines: int = 2) -> List[str]:
    """
    Locate CSV-like contiguous fragments:
    - Group contiguous lines with delimiters (comma/tab) separated by blank lines.
    - Also capture fragments following explicit CSV-like markers.
    """
    lines = text.splitlines()
    blocks = []
    cur = []

    for raw in lines:
        l = raw.rstrip()
        if not l:
            if cur:
                if _is_candidate_csv_block(cur, min_lines):
                    blocks.append("\n".join(cur))
                cur = []
            continue

        if ("," in l) or ("\t" in l) or (";" in l) or ("|" in l):
            cur.append(l)
        else:
            if cur:
                if _is_candidate_csv_block(cur, min_lines):
                    blocks.append("\n".join(cur))
                cur = []
            else:
                # ignore stray non-delimited line
                continue

    if cur and _is_candidate_csv_block(cur, min_lines):
        blocks.append("\n".join(cur))

    # capture blocks after explicit CSV markers (e.g., "# --- CSV-like Block ---")
    for m in re.finditer(r"(?:#\s*[-]*\s*CSV(?:-like)?|#\s*CSV|CSV-like|comma separated)\b[^\n]*\n([\s\S]{0,2000})",
                         text, flags=re.I):
        block = m.group(1)
        block = block.split("\n\n")[0]  # up to next double newline
        candidate_lines = [l for l in block.splitlines() if l.strip()]
        if len(candidate_lines) >= min_lines:
            candidate = "\n".join(candidate_lines)
            if candidate not in blocks and _is_candidate_csv_block(candidate_lines, min_lines):
                blocks.append(candidate)

    return blocks


def parse_csv_block(text: str) -> pd.DataFrame:
    """
    Parse a CSV-like block using csv.Sniffer and pandas. Return empty DF on failure.
    """
    try:
        lines = [l for l in text.splitlines() if l.strip()]
        if not lines:
            return pd.DataFrame()
        sample = "\n".join(lines[:10])

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[',', '\t', ';', '|'])
            delimiter = getattr(dialect, "delimiter", ",")
            has_header = csv.Sniffer().has_header(sample)
        except Exception as e:
            logger.debug("csv.Sniffer failed: %s", e)
            comma_count = sample.count(",")
            tab_count = sample.count("\t")
            delimiter = "," if comma_count >= tab_count else "\t"
            has_header = True

        df = pd.read_csv(io.StringIO("\n".join(lines)), delimiter=delimiter, header=0 if has_header else None)
        if not has_header:
            df.columns = [f"col_{i}" for i in range(len(df.columns))]
        if df.shape[0] >= 1 and df.shape[1] >= 1:
            return df
    except Exception as e:
        logger.debug("pd.read_csv failed on candidate block: %s", e)

    # fallback try
    try:
        df = pd.read_csv(io.StringIO(text))
        if df.shape[0] >= 1 and df.shape[1] >= 1:
            return df
    except Exception as e:
        logger.debug("fallback pd.read_csv failed: %s", e)

    return pd.DataFrame()


# ---------------------------------------------------------
# Key:Value detection
# ---------------------------------------------------------
def _extract_kv_block(text: str) -> str:
    """
    Return the first contiguous key:value area near top of file (common pattern).
    """
    lines = text.splitlines()
    kv_lines = []
    start_idx = None
    for i, l in enumerate(lines):
        if ":" in l and len(l.split(":", 1)[0].strip()) > 0:
            if start_idx is None:
                start_idx = i
            kv_lines.append(l)
        elif start_idx is not None:
            break
    return "\n".join(kv_lines).strip()


def parse_kv_block(text: str) -> pd.DataFrame:
    if not text:
        return pd.DataFrame()
    d = {}
    for line in text.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            d[k.strip()] = v.strip()
    return pd.DataFrame([d]) if d else pd.DataFrame()


# ---------------------------------------------------------
# YAML frontmatter extraction (for markdown files)
# ---------------------------------------------------------
def _extract_yaml_frontmatter(text: str) -> Dict[str, Any]:
    """
    Extract YAML frontmatter from markdown files.
    Returns dict with frontmatter data or empty dict.
    """
    # Match YAML frontmatter between --- markers
    pattern = r'^---\s*\n(.*?)\n---\s*\n'
    match = re.search(pattern, text, re.DOTALL | re.MULTILINE)
    
    if not match:
        return {}
    
    yaml_content = match.group(1)
    result = {}
    
    # Simple YAML parser (handles key: value and lists)
    for line in yaml_content.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()
            
            # Remove quotes
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            elif value.startswith("'") and value.endswith("'"):
                value = value[1:-1]
            
            # Handle lists (simple detection)
            if value.startswith('[') and value.endswith(']'):
                # Simple list parsing
                items = value[1:-1].split(',')
                result[key] = [item.strip().strip('"\'') for item in items]
            else:
                result[key] = value
    
    return result


def parse_yaml_frontmatter(text: str) -> pd.DataFrame:
    """Parse YAML frontmatter and return as DataFrame."""
    yaml_data = _extract_yaml_frontmatter(text)
    if yaml_data:
        return pd.DataFrame([yaml_data])
    return pd.DataFrame()


# ---------------------------------------------------------
# HTML cleaning (remove JS, comments, etc.)
# ---------------------------------------------------------
def _clean_html_text(text: str) -> str:
    """
    Remove JavaScript snippets, HTML comments, and script tags from text.
    """
    # Remove script tags and their content
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    
    # Remove style tags
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    
    # Remove HTML comments
    text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
    
    # Remove inline JavaScript (onclick, onload, etc.)
    text = re.sub(r'\s+on\w+\s*=\s*["\'][^"\']*["\']', '', text, flags=re.IGNORECASE)
    
    return text


# ---------------------------------------------------------
# Master parts extractor
# ---------------------------------------------------------
def smart_read_parts(path: str) -> dict:
    """
    Return dict of DataFrames for sources:
    { "json": df, "html": df, "csv": df, "kv": df, "yaml": df, "raw_text": df }

    Important: extract JSON and HTML blocks first and remove them from the
    working text so the CSV finder doesn't misinterpret JSON lines as CSV rows.
    """
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    parts = {
        "json": pd.DataFrame(),
        "html": pd.DataFrame(),
        "csv": pd.DataFrame(),
        "kv": pd.DataFrame(),
        "yaml": pd.DataFrame(),
        "raw_text": pd.DataFrame(),
    }
    
    # Clean HTML text (remove JS, comments) for better extraction
    cleaned_text = _clean_html_text(text)

    # -------------------------
    # 1) Extract JSON blocks and remove them from working text
    # -------------------------
    json_blocks = _extract_json_blocks(text)
    for jb in json_blocks:
        df = parse_json_block(jb)
        if not df.empty:
            parts["json"] = pd.concat([parts["json"], df], ignore_index=True, sort=False)

    # remove JSON blocks from text (replace with newline placeholders to keep line count)
    cleaned_text = text
    for jb in json_blocks:
        cleaned_text = cleaned_text.replace(jb, "\n" * jb.count("\n"))

    # -------------------------
    # 2) Extract YAML frontmatter (for markdown files)
    # -------------------------
    yaml_df = parse_yaml_frontmatter(text)
    if not yaml_df.empty:
        parts["yaml"] = yaml_df
    
    # -------------------------
    # 3) Extract HTML table blocks and remove them from working text
    # -------------------------
    table_blocks = _extract_table_blocks(cleaned_text)
    for tb in table_blocks:
        df = parse_html_table(tb)
        if not df.empty:
            parts["html"] = pd.concat([parts["html"], df], ignore_index=True, sort=False)

    # remove HTML blocks from cleaned_text as well
    for tb in table_blocks:
        cleaned_text = cleaned_text.replace(tb, "\n" * tb.count("\n"))

    # -------------------------
    # 4) Extract CSV blocks from the cleaned_text (so JSON/HTML noise is gone)
    # -------------------------
    for cb in _extract_csv_blocks(cleaned_text):
        df = parse_csv_block(cb)
        if not df.empty:
            parts["csv"] = pd.concat([parts["csv"], df], ignore_index=True, sort=False)

    # -------------------------
    # 5) Key:Value block from original text (we keep this as-is)
    # -------------------------
    kv_text = _extract_kv_block(text)
    parts["kv"] = parse_kv_block(kv_text)

    # -------------------------
    # 6) Raw fallback (if nothing found)
    # -------------------------
    if (parts["json"].empty and parts["html"].empty and parts["csv"].empty 
        and parts["kv"].empty and parts["yaml"].empty):
        parts["raw_text"] = pd.DataFrame({"text": [text[:2000]]})

    return parts


# ---------------------------------------------------------
# Convenience combined reader
# ---------------------------------------------------------
def smart_read_combined(path: str) -> pd.DataFrame:
    """
    Combine all non-empty part DataFrames (vertical concat, union of columns).
    """
    parts = smart_read_parts(path)
    dfs = [df for df in parts.values() if not df.empty]
    if not dfs:
        return pd.DataFrame()
    try:
        combined = pd.concat(dfs, ignore_index=True, sort=False)
        return combined
    except Exception as e:
        logger.debug("concat failed, returning first non-empty part: %s", e)
        return dfs[0]


# Backwards compat alias (some code expected smart_read)
def smart_read(path: str, ext: str = None) -> pd.DataFrame:
    return smart_read_combined(path)
