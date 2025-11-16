import json
import csv
import re
import yaml
from io import StringIO

# ----------------------------
# Validators (structured & unstructured)
# ----------------------------

def validate_json(text):
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return {"type": "json", "schema": list(obj.keys())}
        if isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], dict):
            return {"type": "json", "schema": list(obj[0].keys())}
    except:
        return None
    return None

def validate_malformed_json(text):
    if "{" in text and "}" in text and ":" in text:
        return {"type": "malformed_json", "schema": None}
    return None

def validate_csv(text):
    try:
        f = StringIO(text)
        reader = csv.reader(f)
        headers = next(reader)
        if len(headers) > 1:
            return {"type": "csv", "schema": headers}
    except:
        return None
    return None

def validate_yaml(text):
    try:
        obj = yaml.safe_load(text)
        if isinstance(obj, dict):
            return {"type": "yaml", "schema": list(obj.keys())}
    except:
        return None
    return None

def validate_kvp(text):
    pairs = re.findall(r"(\w+)=([^ \n]+)", text)
    if len(pairs) > 2:
        return {"type": "kvp", "schema": [p[0] for p in pairs]}
    return None

def validate_free_text(text):
    if len(text.split()) > 20:
        return {"type": "free_text", "schema": None}
    return None

def validate_html(text):
    if "<table" in text.lower() and "<tr" in text.lower():
        headers = re.findall(r"<th[^>]*>(.*?)</th>", text, re.I)
        return {"type": "html_table", "schema": headers}
    return None

def validate_js(text):
    if "function" in text or "console.log" in text:
        return {"type": "javascript", "schema": None}
    return None

def validate_sql(text):
    sql_keywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "FROM", "WHERE"]
    if any(kw in text.upper() for kw in sql_keywords):
        return {"type": "sql", "schema": None}
    return None

def validate_ocr(text):
    if re.search(r"[0O][1Il]|[§¶©]|[=]{3,}", text):
        return {"type": "ocr_noise", "schema": None}
    return None

# ----------------------------
# Export all validators
# ----------------------------
VALIDATORS = [
    validate_json,
    validate_malformed_json,
    validate_csv,
    validate_yaml,
    validate_kvp,
    validate_free_text,
    validate_html,
    validate_js,
    validate_sql,
    validate_ocr
]

# ----------------------------
# File acceptance (structured + unstructured)
# ----------------------------
ALLOWED_CONTENT_TYPES = [
    "application/json",
    "text/csv",
    "text/plain",
    "application/x-yaml",
    "text/yaml",
]

def is_allowed_file(content_type: str) -> bool:
    return content_type in ALLOWED_CONTENT_TYPES
