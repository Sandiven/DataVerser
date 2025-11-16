"""
Extract layer for the ETL pipeline.

Handles file reading and data extraction from various formats.
"""

from .extract import extract_data, detect_file_type
from .smart_readers import smart_read_parts, smart_read_combined
from .file_handlers import READERS

__all__ = ['extract_data', 'detect_file_type', 'smart_read_parts', 'smart_read_combined', 'READERS']

