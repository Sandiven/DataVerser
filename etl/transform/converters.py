"""
Converters module for the transform layer.

Responsible for final, authoritative type conversions before
loading into the warehouse or downstream systems.

Typical tasks:
- Convert columns to specific dtypes
- Enforce final schema types (int, float, bool, datetime, category, string)
- Coerce data to safe formats expected by the destination

Main public function:
    convert_types(df: pd.DataFrame) -> pd.DataFrame
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
#  Helper Conversion Functions
# ---------------------------------------------------------

def convert_to_int(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Convert selected columns to integer (nullable int)."""
    df = df.copy()
    for col in columns:
        if col in df.columns:
            logger.debug(f"Converting '{col}' → int")
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def convert_to_float(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Convert selected columns to float."""
    df = df.copy()
    for col in columns:
        if col in df.columns:
            logger.debug(f"Converting '{col}' → float")
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def convert_to_bool(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Convert selected columns to boolean."""
    df = df.copy()
    true_values = {"true", "1", "yes", "y", "t"}
    false_values = {"false", "0", "no", "n", "f"}

    for col in columns:
        if col in df.columns:
            logger.debug(f"Converting '{col}' → bool")
            df[col] = (
                df[col]
                .astype("string")
                .str.lower()
                .map(lambda v: True if v in true_values else False if v in false_values else pd.NA)
                .astype("boolean")
            )
    return df


def convert_to_string(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Convert selected columns to string dtype."""
    df = df.copy()
    for col in columns:
        if col in df.columns:
            logger.debug(f"Converting '{col}' → string")
            df[col] = df[col].astype("string")
    return df


def convert_to_datetime(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Convert selected columns to pandas datetime with coercion."""
    df = df.copy()
    for col in columns:
        if col in df.columns:
            logger.debug(f"Converting '{col}' → datetime")
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def convert_to_category(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Convert selected columns to category dtype."""
    df = df.copy()
    for col in columns:
        if col in df.columns:
            logger.debug(f"Converting '{col}' → category")
            df[col] = df[col].astype("category")
    return df


# ---------------------------------------------------------
#  Main Conversion Pipeline
# ---------------------------------------------------------

def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Final authoritative type conversion for each column.
    Modify the schema definition below to match your data model.
    """

    logger.info("Running type conversion pipeline...")
    df = df.copy()

    # ----------------------------------------
    # Schema definition (customize for your domain!)
    # ----------------------------------------

    int_columns = ["id", "age", "quantity"]
    float_columns = ["price", "amount"]
    bool_columns = ["is_active", "is_deleted"]
    string_columns = ["name", "category", "country_code", "postal_code"]
    datetime_columns = ["created_at", "updated_at", "dob"]
    category_columns = ["age_group", "status"]

    # ----------------------------------------
    # Apply conversions
    # ----------------------------------------

    df = convert_to_int(df, int_columns)
    df = convert_to_float(df, float_columns)
    df = convert_to_bool(df, bool_columns)
    df = convert_to_string(df, string_columns)
    df = convert_to_datetime(df, datetime_columns)
    df = convert_to_category(df, category_columns)

    logger.info("Type conversion pipeline complete")
    logger.debug(f"Final schema:\n{df.dtypes}")

    return df
