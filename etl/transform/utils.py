"""
Utility helpers for the ETL transform layer.

Used across:
- cleaning.py
- validators.py
- normalization.py
- enrichment.py
- converters.py
- transform_main.py
"""

import logging
import time
import pandas as pd


logger = logging.getLogger(__name__)


# ---------------------------------------------------------
#  Safe Access Helpers
# ---------------------------------------------------------

def get_safe(df: pd.DataFrame, column: str, default=None):
    """Safely get a column; return default if missing."""
    return df[column] if column in df.columns else default


def has_columns(df: pd.DataFrame, cols: list) -> bool:
    """Check whether DataFrame has all required columns."""
    return all(col in df.columns for col in cols)


def missing_columns(df: pd.DataFrame, cols: list) -> list:
    """Return missing columns from DataFrame."""
    return [col for col in cols if col not in df.columns]


# ---------------------------------------------------------
#  Decorators
# ---------------------------------------------------------

def log_step(func):
    """
    Decorator to log start/end + execution time for any transform step.
    Use this on enrichment, normalization, conversion functions, etc.
    """
    def wrapper(*args, **kwargs):
        step_name = func.__name__
        logger.info(f"→ Starting: {step_name}")
        start = time.time()

        try:
            result = func(*args, **kwargs)
        except Exception as e:
            logger.exception(f"❌ Error in step: {step_name}")
            raise e

        duration = round((time.time() - start) * 1000, 2)
        logger.info(f"✓ Finished: {step_name} ({duration} ms)")
        return result

    return wrapper


# ---------------------------------------------------------
#  DataFrame Inspection Helpers
# ---------------------------------------------------------

def df_info(df: pd.DataFrame) -> str:
    """
    Return human-readable DataFrame stats as a string.
    Useful for debugging/logging.
    """
    return (
        f"Rows: {len(df)}, Cols: {df.shape[1]}, "
        f"Columns: {list(df.columns)}"
    )


def preview_df(df: pd.DataFrame, n=5) -> pd.DataFrame:
    """Return a small preview without printing to logs."""
    return df.head(n)


def log_df_preview(df: pd.DataFrame, n=5):
    """Log a small preview of the DataFrame for debugging."""
    logger.debug(f"Preview first {n} rows:\n{df.head(n)}")


# ---------------------------------------------------------
#  Schema Helpers
# ---------------------------------------------------------

def enforce_column_order(df: pd.DataFrame, ordered_cols: list) -> pd.DataFrame:
    """
    Reorder DataFrame columns safely,
    leaving extra/unexpected columns at the end.
    """
    df = df.copy()
    final_cols = [c for c in ordered_cols if c in df.columns] + \
                 [c for c in df.columns if c not in ordered_cols]
    return df[final_cols]


# ---------------------------------------------------------
#  Error Formatting
# ---------------------------------------------------------

def format_error(message: str, details: dict = None) -> str:
    """
    Produce a consistent error message with optional details.
    """
    base = f"[ERROR] {message}"
    if details:
        base += " | Details: " + ", ".join([f"{k}={v}" for k, v in details.items()])
    return base


# ---------------------------------------------------------
#  Timing Utilities
# ---------------------------------------------------------

def time_ms() -> float:
    """Return current time in milliseconds."""
    return time.time() * 1000


def measure(fn, *args, **kwargs):
    """
    Run a function and return (result, duration_ms).
    Useful in debugging or profiling transform steps.
    """
    start = time_ms()
    result = fn(*args, **kwargs)
    duration = time_ms() - start
    return result, round(duration, 2)
