from pydantic import BaseModel
from typing import Optional, List, Any


class QueryRequest(BaseModel):
    source_id: str
    mode: str                     # "mongo" | "mongo_nl" | "semantic"
    filter: Optional[dict] = None
    query: Optional[str] = None
    async_mode: bool = False


class QueryResponse(BaseModel):
    query_id: str
    status: str
    results: Optional[List[Any]] = None
    result_count: Optional[int] = None
