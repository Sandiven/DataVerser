# etl/extract/file_handlers.py
import json
import pandas as pd
import logging
from .smart_readers import smart_read_combined
from .pdf_readers import read_pdf_tables, read_pdf_text_ocr

logger = logging.getLogger(__name__)

def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return pd.DataFrame(data)

def read_html_safely(path):
    """
    Try multiple engines/parsers for read_html and return the first table found.
    """
    try:
        # preferred: use lxml parser (fast)
        tables = pd.read_html(path, flavor="bs4", parser="lxml")
        if tables:
            return tables[0]
    except Exception as e:
        logger.debug(f"read_html with lxml failed: {e}")

    try:
        # fallback to html5lib if installed
        tables = pd.read_html(path, flavor="bs4", parser="html5lib")
        if tables:
            return tables[0]
    except Exception as e:
        logger.debug(f"read_html with html5lib failed: {e}")

    # last resort: try pandas default (may still raise)
    try:
        tables = pd.read_html(path)
        if tables:
            return tables[0]
    except Exception as e:
        logger.warning(f"All read_html attempts failed for {path}: {e}")
        return pd.DataFrame()

def read_xml_safely(path):
    try:
        df = pd.read_xml(path)
        return df
    except Exception as e:
        logger.warning(f"pd.read_xml failed: {e}")
        return pd.DataFrame()

READERS = {
    "json": read_json,
    "csv": pd.read_csv,
    "txt": lambda path: smart_read_combined(path),
    "html": lambda path: smart_read_combined(path),
    "md": lambda path: smart_read_combined(path),
    "xml": lambda path: smart_read_combined(path),
    "xlsx": lambda path: pd.read_excel(path, engine="openpyxl"),
    "xls": lambda path: pd.read_excel(path, engine="xlrd", dtype=str),
    "tsv": lambda path: pd.read_csv(path, sep="\t"),
    "pdf": lambda path: (read_pdf_tables(path) or read_pdf_text_ocr(path)),
    "parquet": pd.read_parquet,
}