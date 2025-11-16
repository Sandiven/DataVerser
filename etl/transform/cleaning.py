"""
Cleaning module for the transform layer.

This module contains functions that clean raw data before
validation, normalization, enrichment, or conversion.

Main public function:
    clean_dataframe(df: pd.DataFrame) -> pd.DataFrame
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
#  Helper Cleaning Functions
# ---------------------------------------------------------

def standardize_column_names(df):
    # Ensure all column names are strings
    df.columns = df.columns.map(lambda x: str(x).strip())

    # Now safely apply string transformations
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^a-zA-Z0-9_]", "", regex=True)
    )
    return df



def trim_string_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove leading/trailing whitespace from object (string-like) columns.
    """
    logger.debug("Trimming string fields...")
    df = df.copy()

    string_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in string_cols:
        df[col] = df[col].astype("string").str.strip()

    return df


def replace_empty_strings_with_nan(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert empty strings and whitespace-only strings to NaN.
    """
    logger.debug("Replacing empty strings with NaN...")

    df = df.copy()
    df = df.replace(r"^\s*$", np.nan, regex=True)

    return df


def remove_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows.
    """
    logger.debug("Removing duplicate rows...")

    before = len(df)
    df = df.drop_duplicates()
    after = len(df)

    logger.debug(f"Removed {before - after} duplicate rows")

    return df


def drop_fully_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows where all values are NaN.
    """
    logger.debug("Dropping fully empty rows...")

    before = len(df)
    df = df.dropna(how="all")
    after = len(df)

    logger.debug(f"Dropped {before - after} empty rows")

    return df


# ---------------------------------------------------------
#  Public API
# ---------------------------------------------------------

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all cleaning steps in a controlled and logged order.

    Parameters:
        df (pd.DataFrame): Raw input DataFrame.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """

    logger.info("Running cleaning pipeline...")

    df = standardize_column_names(df)
    logger.debug("Column names standardized")

    df = trim_string_fields(df)
    logger.debug("String fields trimmed")

    df = replace_empty_strings_with_nan(df)
    logger.debug("Empty strings replaced with NaN")

    df = remove_duplicate_rows(df)
    logger.debug("Duplicate rows removed")

    df = drop_fully_empty_rows(df)
    logger.debug("Fully empty rows dropped")

    logger.info("Cleaning pipeline complete")
    logger.debug(f"Cleaned DataFrame: {len(df)} rows, {df.shape[1]} columns")

    return df
