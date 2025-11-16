import sys
import os

# ------------------------------------------
# Add backend root (parent of app/) to PYTHONPATH
# Allows importing ETL modules from root etl/ folder
# ------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# Add ETL root directory to path
ETL_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..", "etl"))
if ETL_ROOT not in sys.path:
    sys.path.insert(0, ETL_ROOT)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routes.upload_router import router as upload_router
from app.routes.schema_router import router as schema_router
from app.routes.schema_history_router import router as schema_history_router
from app.routes.logs_router import router as logs_router
from app.routes.query_router import router as query_router
from app.routes.metrics_router import router as metrics_router

from app.database import create_indexes
from app.routes.debug_router import router as debug_router


app = FastAPI(title="Dataverse Backend")

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check endpoint
@app.get("/")
async def root():
    return {"message": "Backend is running!", "status": "ok"}

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "Dataverse Backend"}

# ROUTES
app.include_router(upload_router)
app.include_router(schema_router)
app.include_router(schema_history_router)
app.include_router(logs_router)
app.include_router(query_router)
app.include_router(metrics_router)
app.include_router(debug_router)


@app.on_event("startup")
async def startup_event():
    try:    
        await create_indexes()
    except Exception as e:
        print("Warning: create_indexes() failed:", str(e))
