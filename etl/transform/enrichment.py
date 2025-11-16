"""
Enrichment module for the transform layer.

This module adds NEW information to the DataFrame such as:
- Derived or calculated fields
- External lookup values (dimension tables, dictionaries)
- Geographic or category enrichment
- Business rule–based enrichment

Main public function:
    enrich(df: pd.DataFrame) -> pd.DataFrame
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
#  Example 1: Derived Fields
# ---------------------------------------------------------

def add_full_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create `full_name` from first and last name if columns exist.
    """
    if {"first_name", "last_name"}.issubset(df.columns):
        logger.debug("Adding full_name field...")
        df = df.copy()
        df["full_name"] = (
            df["first_name"].astype("string").str.strip() + " " +
            df["last_name"].astype("string").str.strip()
        )
    else:
        logger.debug("Skipping full_name enrichment (missing columns)")
    return df


# ---------------------------------------------------------
#  Example 2: Add Age Group
# ---------------------------------------------------------

def add_age_group(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add an age_group column based on age value.
    """
    if "age" not in df.columns:
        logger.debug("Skipping age_group enrichment (missing 'age')")
        return df

    logger.debug("Adding age_group field...")
    df = df.copy()
    df["age_group"] = pd.cut(
        df["age"],
        bins=[0, 17, 29, 44, 59, 100, 200],
        labels=["child", "young_adult", "adult", "middle_aged", "senior", "unknown"],
        include_lowest=True
    )

    return df


# ---------------------------------------------------------
#  Example 3: Reference Table Lookup
# ---------------------------------------------------------

def enrich_country_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert ISO country codes into full names via lookup.
    """

    country_lookup = {
        "US": "United States",
        "UK": "United Kingdom",
        "IN": "India",
        "DE": "Germany",
        "FR": "France",
        # ... extend as needed
    }

    if "country_code" not in df.columns:
        logger.debug("Skipping country enrichment (missing country_code)")
        return df

    logger.debug("Adding country_name based on country_code...")
    df = df.copy()
    df["country_name"] = df["country_code"].map(country_lookup).fillna("Unknown")

    return df


# ---------------------------------------------------------
#  Example 4: Conditional Business Rule Enrichment
# ---------------------------------------------------------

def add_is_active(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a boolean `is_active` based on updated_at.
    Example rule: record active if updated within past 365 days.
    """

    if "updated_at" not in df.columns:
        logger.debug("Skipping is_active enrichment (missing updated_at)")
        return df

    logger.debug("Adding is_active field...")
    df = df.copy()

    # if updated_at is datetime
    if pd.api.types.is_datetime64_any_dtype(df["updated_at"]):
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=365)
        df["is_active"] = df["updated_at"] >= cutoff
    else:
        logger.debug("'updated_at' is not datetime — cannot compute is_active")

    return df


# ---------------------------------------------------------
#  Main Enrichment Pipeline
# ---------------------------------------------------------

def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich the DataFrame with additional, non-destructive information.
    """

    logger.info("Running enrichment pipeline...")
    df = df.copy()

    # 1. Derived name fields
    df = add_full_name(df)

    # 2. Age group classification
    df = add_age_group(df)

    # 3. Lookup country name from country code
    df = enrich_country_name(df)

    # 4. Add business rule flags
    df = add_is_active(df)

    logger.info("Enrichment pipeline complete")
    logger.debug(f"Enriched DataFrame: {len(df)} rows, {df.shape[1]} columns")

    return df
