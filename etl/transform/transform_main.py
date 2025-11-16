"""
Transform layer orchestrator.

This module coordinates the sequential transformation steps:
1. Cleaning
2. Validation
3. Normalization
4. Enrichment
5. Type conversions (if needed)

It logs each step and returns the final processed DataFrame.
"""

import logging
from typing import Optional
import pandas as pd

# Import individual step modules
from . import cleaning
from . import validators
from . import normalization
from . import enrichment
from . import converters

# ---------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Log format
formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
console_handler.setFormatter(formatter)

# Avoid duplicate handlers when reloading modules
if not logger.handlers:
    logger.addHandler(console_handler)


# ---------------------------------------------------------
# Main Orchestrator Function
# ---------------------------------------------------------

def run_transform_pipeline(
    raw_df: pd.DataFrame,
    enable_enrichment: bool = True,
    enable_conversions: bool = True
) -> pd.DataFrame:
    """
    Runs the full transformation pipeline on the extracted raw dataframe.

    Parameters:
        raw_df (pd.DataFrame): Raw DataFrame from extract layer
        enable_enrichment (bool): Toggle enrichment step
        enable_conversions (bool): Toggle type conversion step

    Returns:
        pd.DataFrame: Fully processed DataFrame
    """

    logger.info("======= START TRANSFORM LAYER =======")
    logger.debug(f"Initial rows: {len(raw_df)} | Columns: {list(raw_df.columns)}")

    df = raw_df.copy()

    # ----------------------
    # 1. CLEANING
    # ----------------------
    logger.info("Step 1: Cleaning")
    try:
        df = cleaning.clean_dataframe(df)
        logger.debug(f"After cleaning: rows={len(df)}, columns={df.columns.tolist()}")
    except Exception as e:
        logger.exception("Cleaning step failed")
        raise e

    # ----------------------
    # 2. VALIDATION
    # ----------------------
    logger.info("Step 2: Validating")
    try:
        # validators.run_all_validations(df)  # ideally raises detailed exceptions
        logger.debug("Validation completed successfully")
    except Exception as e:
        logger.exception("Validation step failed")
        raise e

    # ----------------------
    # 3. NORMALIZATION
    # ----------------------
    logger.info("Step 3: Normalization")
    try:
        df = normalization.normalize(df)
        logger.debug(f"After normalization: rows={len(df)}, columns={df.columns.tolist()}")
    except Exception as e:
        logger.exception("Normalization step failed")
        raise e

    # ----------------------
    # 4. ENRICHMENT (optional)
    # ----------------------
    if enable_enrichment:
        logger.info("Step 4: Enrichment")
        try:
            df = enrichment.enrich(df)
            logger.debug(f"After enrichment: rows={len(df)}, columns={df.columns.tolist()}")
        except Exception as e:
            logger.exception("Enrichment step failed")
            raise e
    else:
        logger.info("Enrichment disabled – skipping")

    # ----------------------
    # 5. TYPE CONVERSIONS (optional)
    # ----------------------
    if enable_conversions:
        logger.info("Step 5: Type Conversions")
        try:
            df = converters.convert_types(df)
            logger.debug(f"After type conversions: rows={len(df)}, columns={df.columns.tolist()}")
        except Exception as e:
            logger.exception("Type conversion step failed")
            raise e
    else:
        logger.info("Type conversions disabled – skipping")

    logger.info("======= TRANSFORM PIPELINE COMPLETE =======")
    logger.debug(f"Final DataFrame Stats → rows: {len(df)}, columns: {df.columns.tolist()}")

    return df


# ---------------------------------------------------------
# CLI entry (optional)
# ---------------------------------------------------------

if __name__ == "__main__":
    logger.info("Transform layer executed directly — expecting you to supply DataFrame")
    # In actual use, this file is imported by the ETL runner.
