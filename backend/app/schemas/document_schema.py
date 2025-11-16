from pydantic import BaseModel
from datetime import datetime
from typing import List, Dict

class DocumentSchema(BaseModel):
    filename: str
    schema: Dict
    uploaded_at: datetime
    logs: List[str] = []
