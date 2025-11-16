from typing import List
from datetime import datetime

class DocumentModel:
    def __init__(self, filename: str, schema: dict, uploaded_at: datetime, logs: List[str]):
        self.filename = filename
        self.schema = schema
        self.uploaded_at = uploaded_at
        self.logs = logs
