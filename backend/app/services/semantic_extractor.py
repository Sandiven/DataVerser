import re
from typing import Dict, Any
from io import BytesIO

# try optional PDF reader for better PDF text extraction
try:
    from PyPDF2 import PdfReader
    _HAS_PYPDF2 = True
except Exception:
    _HAS_PYPDF2 = False


# -----------------------
# Helpers: text extraction
# -----------------------
def _text_from_pdf_bytes(data: bytes) -> str:
    """
    Try to extract text from PDF bytes using PyPDF2 if available.
    If PyPDF2 isn't available, return empty string to let fallback happen.
    """
    if not _HAS_PYPDF2:
        return ""
    try:
        reader = PdfReader(BytesIO(data))
        texts = []
        for page in reader.pages:
            try:
                texts.append(page.extract_text() or "")
            except Exception:
                continue
        return "\n".join(texts)
    except Exception:
        return ""


def _normalize_text_bytes(data: bytes) -> str:
    """
    Given raw file bytes, try to return a best-effort text string:
    - If looks like PDF use PDF extraction
    - Otherwise decode with fallback encodings
    """
    # quick PDF check
    if data[:4] == b"%PDF":
        txt = _text_from_pdf_bytes(data)
        if txt:
            return txt

    # else try decode with utf-8, latin1 fallback
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="ignore")


# -----------------------
# Regex patterns
# -----------------------
_EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
_PHONE_RE = re.compile(r"(?:\+?\d{1,3}[\s\-\.])?(?:\(?\d{2,4}\)?[\s\-\.])?\d{3,4}[\s\-\.]?\d{3,4}")
_URL_RE = re.compile(r"https?://[^\s,;]+")
_DATE_RE = re.compile(
    r"\b(?:\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}|\d{1,2}\s(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s\d{4})\b",
    re.IGNORECASE
)
_KVP_RE = re.compile(r"(?m)^\s*([A-Za-z0-9 _\-/]{2,50})\s*[:=]\s*(.+)$")


# -----------------------
# Semantic extraction core
# -----------------------
def extract_semantic_fields_from_text(text: str) -> Dict[str, Any]:
    result = {
        "emails": [],
        "phones": [],
        "urls": [],
        "dates": [],
        "kvp": {},
        "headings": [],
        "word_count": 0,
        "char_count": 0
    }

    if not text or not text.strip():
        return result

    norm = re.sub(r"\r\n", "\n", text)
    norm = re.sub(r"[ \t]+", " ", norm)

    result["char_count"] = len(norm)
    words = re.findall(r"\w+", norm)
    result["word_count"] = len(words)

    # emails
    result["emails"] = list(dict.fromkeys(_EMAIL_RE.findall(norm)))

    # urls
    result["urls"] = list(dict.fromkeys(_URL_RE.findall(norm)))

    # phones
    phones = _PHONE_RE.findall(norm)
    phones = [p for p in phones if len(re.sub(r"\D", "", p)) >= 7]
    result["phones"] = list(dict.fromkeys(phones))

    # dates
    result["dates"] = list(dict.fromkeys(_DATE_RE.findall(norm)))

    # key-value pairs
    for m in _KVP_RE.finditer(norm):
        k = m.group(1).strip()
        v = m.group(2).strip()
        result["kvp"].setdefault(k, []).append(v)

    # headings
    lines = [ln.strip() for ln in norm.split("\n") if ln.strip()]
    headings = []

    for ln in lines:
        wc = len(ln.split())
        if 1 < wc < 8:
            if ln == ln.upper() or (ln[0].isupper() and any(ch.isupper() for ch in ln[1:])):
                headings.append(ln)

    result["headings"] = list(dict.fromkeys(headings))

    return result


def extract_semantic_schema(file_bytes: bytes, filename: str = None) -> Dict[str, Any]:
    text = _normalize_text_bytes(file_bytes)
    sem = extract_semantic_fields_from_text(text)

    metadata = {
        "filename": filename,
        "char_count": sem.get("char_count", 0),
        "word_count": sem.get("word_count", 0)
    }

    return {
        "metadata": metadata,
        "entities": {
            "emails": sem.get("emails", []),
            "phones": sem.get("phones", []),
            "urls": sem.get("urls", []),
            "dates": sem.get("dates", [])
        },
        "kvp": sem.get("kvp", {}),
        "headings": sem.get("headings", []),
        "counts": {
            "words": sem.get("word_count", 0),
            "chars": sem.get("char_count", 0)
        }
    }


# -----------------------
# REQUIRED CLASS (Fixes your error)
# -----------------------
class SemanticExtractor:
    """
    Thin wrapper class so query_service can import and use this cleanly.
    """

    def extract_from_text(self, text: str):
        return extract_semantic_fields_from_text(text)

    def extract_from_bytes(self, file_bytes: bytes, filename: str = None):
        return extract_semantic_schema(file_bytes, filename)
