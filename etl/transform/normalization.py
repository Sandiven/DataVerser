"""
Normalization module for the transform layer.

This module handles:
- Numeric normalization (type casting, coercion)
- Date parsing
- Standardizing categorical values
- Lowercasing / formatting certain fields
- Normalizing IDs, postal codes, country codes, etc. (as needed)

Main public function:
    normalize(df: pd.DataFrame) -> pd.DataFrame
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
#  Numeric Normalization
# ---------------------------------------------------------

def normalize_numeric_columns(df: pd.DataFrame, numeric_cols: list) -> pd.DataFrame:
    """
    Convert columns to numeric, coercing errors to NaN.
    """
    logger.debug("Normalizing numeric columns...")

    df = df.copy()
    for col in numeric_cols:
        if col in df.columns:
            logger.debug(f"Converting '{col}' to numeric...")
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ---------------------------------------------------------
#  Datetime Normalization
# ---------------------------------------------------------

def normalize_datetime_columns(df: pd.DataFrame, datetime_cols: list) -> pd.DataFrame:
    """
    Convert date/time columns to pandas datetime format.
    """
    logger.debug("Normalizing datetime columns...")

    df = df.copy()
    for col in datetime_cols:
        if col in df.columns:
            logger.debug(f"Parsing datetime field '{col}'...")
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


# ---------------------------------------------------------
#  Categorical Normalization
# ---------------------------------------------------------

def standardize_string_columns(df: pd.DataFrame, string_cols: list) -> pd.DataFrame:
    """
    Normalize common categorical/string fields:
    - lowercase text
    - collapse repeated spaces
    - remove punctuation if needed (configurable)
    """
    logger.debug("Normalizing string/categorical columns...")

    df = df.copy()
    for col in string_cols:
        if col in df.columns:
            logger.debug(f"Standardizing '{col}'...")
            df[col] = (
                df[col]
                .astype("string")
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
                .str.lower()
            )

    return df


# ---------------------------------------------------------
#  ID / Code Normalization (optional)
# ---------------------------------------------------------

def normalize_code_fields(df: pd.DataFrame, fields: list) -> pd.DataFrame:
    """
    Normalize fields like country codes, postal codes, category codes.
    - uppercase codes
    - strip whitespace
    """
    logger.debug("Normalizing code-like fields...")

    df = df.copy()
    for col in fields:
        if col in df.columns:
            logger.debug(f"Standardizing code field '{col}'...")
            df[col] = (
                df[col]
                .astype("string")
                .str.strip()
                .str.upper()
            )

    return df


# ---------------------------------------------------------
#  Main Normalization Pipeline
# ---------------------------------------------------------

def _detect_numeric_columns(df: pd.DataFrame) -> list:
    """Dynamically detect columns that should be numeric."""
    numeric_cols = []
    numeric_keywords = ['price', 'amount', 'cost', 'age', 'quantity', 'count', 
                       'num', 'number', 'value', 'score', 'rating', 'views',
                       'stock', 'inventory', 'weight', 'height', 'width', 'depth']
    
    for col in df.columns:
        col_lower = col.lower()
        # Check if column name suggests numeric
        if any(keyword in col_lower for keyword in numeric_keywords):
            numeric_cols.append(col)
        # Check if column can be converted to numeric
        elif df[col].dtype == 'object':
            try:
                # Try converting a sample
                sample = df[col].dropna().head(10)
                if len(sample) > 0:
                    pd.to_numeric(sample, errors='raise')
                    numeric_cols.append(col)
            except:
                pass
    
    return numeric_cols


def _detect_datetime_columns(df: pd.DataFrame) -> list:
    """Dynamically detect columns that should be datetime."""
    datetime_cols = []
    datetime_keywords = ['date', 'time', 'created', 'updated', 'modified', 
                        'timestamp', 'dob', 'birth', 'published', 'released']
    
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in datetime_keywords):
            datetime_cols.append(col)
    
    return datetime_cols


def _detect_code_fields(df: pd.DataFrame) -> list:
    """Dynamically detect code-like fields."""
    code_fields = []
    code_keywords = ['code', 'id', 'key', 'iso', 'country_code', 'postal_code',
                    'zip', 'sku', 'barcode']
    
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in code_keywords):
            code_fields.append(col)
    
    return code_fields


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Master normalization function with dynamic type detection.

    Automatically detects:
    - numeric columns (by name patterns and content)
    - datetime columns (by name patterns)
    - code fields (by name patterns)
    - string/categorical fields (default)
    """

    logger.info("Running normalization pipeline...")
    df = df.copy()

    # ----------------------------------------
    # 1. Dynamically detect column types
    # ----------------------------------------

    numeric_cols = _detect_numeric_columns(df)
    datetime_cols = _detect_datetime_columns(df)
    code_fields = _detect_code_fields(df)
    
    # String columns are those not in other categories
    all_special = set(numeric_cols + datetime_cols + code_fields)
    string_cols = [col for col in df.columns if col not in all_special]

    logger.debug(f"Detected numeric columns: {numeric_cols}")
    logger.debug(f"Detected datetime columns: {datetime_cols}")
    logger.debug(f"Detected code fields: {code_fields}")
    logger.debug(f"String columns: {string_cols[:5]}...")  # Log first 5

    # ----------------------------------------
    # 2. Apply normalization steps in order
    # ----------------------------------------

    if numeric_cols:
        df = normalize_numeric_columns(df, numeric_cols)
        logger.debug("Numeric normalization complete")

    if datetime_cols:
        df = normalize_datetime_columns(df, datetime_cols)
        logger.debug("Datetime normalization complete")

    if string_cols:
        df = standardize_string_columns(df, string_cols)
        logger.debug("String normalization complete")

    if code_fields:
        df = normalize_code_fields(df, code_fields)
        logger.debug("Code-field normalization complete")

    # ----------------------------------------
    # Final log
    # ----------------------------------------
    logger.info("Normalization pipeline complete")
    logger.debug(f"Normalized DataFrame: {len(df)} rows, {df.shape[1]} columns")

    return df
