from fastapi import APIRouter
from app.services.dynamic_etl_adapter import run_dynamic_etl_bytes

router = APIRouter(prefix="/debug", tags=["Debug"])

@router.get("/etl-test")
async def etl_test():
    test_data = b"name,age\nTony,25\nAlice,30"
    result = await run_dynamic_etl_bytes(test_data, "test.csv")
    return {"status": "ok", "etl": result}
