"""
Validation module for the transform layer.

This module contains validation checks to ensure the cleaned data
meets structural and logical expectations BEFORE normalization,
enrichment, or conversions.

Main public function:
    run_all_validations(df: pd.DataFrame)
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
#  Custom Exceptions
# ---------------------------------------------------------

class ValidationError(Exception):
    """Raised when data fails a required validation check."""
    pass


# ---------------------------------------------------------
#  Example Validation Functions
# ---------------------------------------------------------

def check_required_columns(df: pd.DataFrame, required_cols: list):
    """
    Ensure required columns exist in the dataset.
    Raise ValidationError if any are missing.
    """
    logger.debug("Checking required columns...")

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        logger.error(f"Missing required columns: {missing}")
        raise ValidationError(f"Missing required columns: {missing}")

    logger.debug("All required columns present.")


def check_no_nulls_in_key_columns(df: pd.DataFrame, key_cols: list):
    """
    Ensure key business columns do not contain null values.
    """
    logger.debug("Checking nulls in key columns...")

    for col in key_cols:
        if df[col].isna().any():
            count = df[col].isna().sum()
            logger.error(f"Column '{col}' has {count} null values.")
            raise ValidationError(f"Column '{col}' contains null values.")


def check_value_ranges(df: pd.DataFrame, column: str, min_value=None, max_value=None):
    """
    Check that numeric column values fall within expected ranges.
    """
    logger.debug(f"Checking value range for column '{column}'...")

    if column not in df.columns:
        logger.debug(f"Column '{column}' not in DataFrame. Skipping range check.")
        return

    if min_value is not None:
        if (df[column] < min_value).any():
            logger.error(f"Values in '{column}' fall below minimum allowed ({min_value}).")
            raise ValidationError(f"Values in '{column}' fall below minimum allowed ({min_value}).")

    if max_value is not None:
        if (df[column] > max_value).any():
            logger.error(f"Values in '{column}' exceed maximum allowed ({max_value}).")
            raise ValidationError(f"Values in '{column}' exceed maximum allowed ({max_value}).")

    logger.debug(f"Column '{column}' within allowed range.")


def check_unique_column(df: pd.DataFrame, column: str):
    """
    Ensure a column contains unique values (e.g., IDs).
    """
    logger.debug(f"Checking uniqueness for column '{column}'...")

    if df[column].duplicated().any():
        duplicates = df[df[column].duplicated()][column].tolist()
        logger.error(f"Column '{column}' contains duplicate values: {duplicates[:10]}")
        raise ValidationError(f"Duplicate values found in '{column}': {duplicates[:10]}")

    logger.debug(f"Column '{column}' is unique.")


def check_row_count(df: pd.DataFrame, min_rows: int = 1):
    """
    Ensure DataFrame is not empty or too small.
    """
    logger.debug("Checking row count...")

    if len(df) < min_rows:
        logger.error(f"DataFrame has too few rows: {len(df)} (minimum required: {min_rows})")
        raise ValidationError(f"DataFrame must contain at least {min_rows} rows.")

    logger.debug("Row count is valid.")


# ---------------------------------------------------------
#  Master Validation Runner
# ---------------------------------------------------------

def run_all_validations(df: pd.DataFrame):
    """
    Executes all mandatory validation checks in order.
    Raises ValidationError on any failure.
    """

    logger.info("Running validation pipeline...")

    # 1. Dataset must not be empty
    check_row_count(df, min_rows=1)

    # 2. Required columns (modify to fit your real schema)
    required_cols = ["id", "name", "created_at"]   # <-- CUSTOMIZE!
    check_required_columns(df, required_cols)

    # 3. Key fields must not be NULL
    key_cols = ["id"]   # <-- CUSTOMIZE!
    check_no_nulls_in_key_columns(df, key_cols)

    # 4. ID field must be unique
    check_unique_column(df, "id")

    # 5. Example: numeric value constraints (if applicable)
    if "age" in df.columns:
        check_value_ranges(df, "age", min_value=0, max_value=120)

    logger.info("Validation pipeline complete — all checks passed.")
