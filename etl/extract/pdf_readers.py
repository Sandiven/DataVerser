# etl/extract/pdf_readers.py
import pdfplumber
import pandas as pd
from pdf2image import convert_from_path
import pytesseract
import io, os, logging

logger = logging.getLogger(__name__)

def read_pdf_tables(path):
    # try camelot / tabula if installed and Java available (recommend)
    try:
        import camelot
        tables = camelot.read_pdf(path, pages='all')
        if tables and len(tables) > 0:
            dfs = [t.df for t in tables]
            return pd.concat(dfs, ignore_index=True)
    except Exception as e:
        logger.debug("camelot failed: %s", e)

    # fallback to pdfplumber extracting table-like areas
    try:
        all_tables = []
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                for tbl in page.extract_tables():
                    all_tables.append(pd.DataFrame(tbl[1:], columns=tbl[0]))
        if all_tables:
            return pd.concat(all_tables, ignore_index=True)
    except Exception as e:
        logger.debug("pdfplumber table extraction failed: %s", e)

    return pd.DataFrame()

def read_pdf_text_ocr(path):
    # rasterize pages and OCR (slow) - needs poppler and tesseract installed
    try:
        pages = convert_from_path(path, dpi=200)
        text = ""
        for page in pages:
            text += pytesseract.image_to_string(page)
        # use smart_read techniques to extract JSON/CSV from text if present
        from .smart_readers import _extract_json_blocks, _extract_csv_blocks, parse_json_block, parse_csv_block
        json_blocks = _extract_json_blocks(text)
        csv_blocks = _extract_csv_blocks(text)
        dfs = []
        for jb in json_blocks:
            df = parse_json_block(jb)
            if not df.empty:
                dfs.append(df)
        for cb in csv_blocks:
            df = parse_csv_block(cb)
            if not df.empty:
                dfs.append(df)
        if dfs:
            return pd.concat(dfs, ignore_index=True)
    except Exception as e:
        logger.debug("OCR pipeline failed: %s", e)
    return pd.DataFrame()
