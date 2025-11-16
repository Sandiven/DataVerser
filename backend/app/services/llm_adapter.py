# app/services/llm_adapter.py
import re
import json
from typing import Dict, Any, Optional

"""
Mock LLM adapter.

Behavior:
- Interprets simple natural language patterns and returns a Mongo filter dict.
- If it can't parse a meaningful filter, returns {} (match-all) — conservative.
- Later you can replace or extend this file to call a real LLM (OpenAI/etc).
"""

class MockLLMAdapter:
    def __init__(self):
        # add simple mapping keywords if needed
        pass

    async def generate_mongo_filter(self, nl_query: str, schema: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Convert a natural language query (nl_query) to a MongoDB filter (dictionary).
        Very best-effort mock implementation using regex and heuristics.

        Examples it handles:
          - "show all" -> {}
          - "users older than 30" -> {"age": {"$gt": 30}}
          - "price < 100" -> {"price": {"$lt": 100}}
          - "country is India" -> {"country": "India"}
          - "status in (open, closed)" -> {"status": {"$in": ["open", "closed"]}}
        """
        text = nl_query.strip().lower()

        # 1) if user asked to show all / list all
        if re.search(r"\b(show|list|get|all)\b", text) and ("all" in text or "show all" in text or "list all" in text):
            return {}

        # 2) numeric comparisons: "age > 30", "amount greater than 100"
        # patterns: "<field> (greater than|>) N", "<field> (less than|<) N", "<field> = N"
        # Try to find "<field> (>) N"
        m = re.search(r"([a-zA-Z0-9_\.]+)\s*(?:>=|greater than or equal to|at least)\s*([0-9]+(?:\.[0-9]+)?)", text)
        if m:
            field, val = m.group(1), float(m.group(2))
            return {field: {"$gte": _int_if_int(val)}}

        m = re.search(r"([a-zA-Z0-9_\.]+)\s*(?:>|greater than|over|above)\s*([0-9]+(?:\.[0-9]+)?)", text)
        if m:
            field, val = m.group(1), float(m.group(2))
            return {field: {"$gt": _int_if_int(val)}}

        m = re.search(r"([a-zA-Z0-9_\.]+)\s*(?:<=|less than or equal to|at most)\s*([0-9]+(?:\.[0-9]+)?)", text)
        if m:
            field, val = m.group(1), float(m.group(2))
            return {field: {"$lte": _int_if_int(val)}}

        m = re.search(r"([a-zA-Z0-9_\.]+)\s*(?:<|less than|under|below)\s*([0-9]+(?:\.[0-9]+)?)", text)
        if m:
            field, val = m.group(1), float(m.group(2))
            return {field: {"$lt": _int_if_int(val)}}

        m = re.search(r"([a-zA-Z0-9_\.]+)\s*(?:=|is|equals|equal to)\s*([a-zA-Z0-9_\-@\.]+)", text)
        if m:
            field, val = m.group(1), m.group(2)
            # try to make numeric if it looks numeric
            return {field: _maybe_number(val)}

        # 3) "in" lists: "status in (open, closed)" or "status is one of open,closed"
        m = re.search(r"([a-zA-Z0-9_\.]+)\s+(?:in|is one of|one of|is among)\s*[\(\[]?([a-zA-Z0-9_\-,\s]+)[\)\]]?", text)
        if m:
            field = m.group(1)
            vals_raw = m.group(2)
            vals = [v.strip() for v in re.split(r",|\band\b", vals_raw) if v.strip()]
            return {field: {"$in": vals}}

        # 4) "contains" / substring search: "name contains john"
        m = re.search(r"([a-zA-Z0-9_\.]+)\s+(?:contains|has|includes)\s+([a-zA-Z0-9_\-]+)", text)
        if m:
            field, val = m.group(1), m.group(2)
            # use case-insensitive regex search
            return {field: {"$regex": val, "$options": "i"}}

        # 5) boolean: "is_active is true"
        m = re.search(r"([a-zA-Z0-9_\.]+)\s+is\s+(true|false)", text)
        if m:
            field, val = m.group(1), (m.group(2) == "true")
            return {field: val}

        # 6) fallback: try to find "field name <word>" pairs heuristically "country india"
        m = re.search(r"([a-zA-Z0-9_\.]+)\s+([a-zA-Z0-9_\-@\.]+)$", text)
        if m:
            return {m.group(1): _maybe_number(m.group(2))}

        # 7) if nothing matched, return match-all filter (empty dict)
        return {}

def _int_if_int(n: float):
    if n.is_integer():
        return int(n)
    return n

def _maybe_number(s: str):
    try:
        if "." in s:
            v = float(s)
        else:
            v = int(s)
        return v
    except Exception:
        return s
