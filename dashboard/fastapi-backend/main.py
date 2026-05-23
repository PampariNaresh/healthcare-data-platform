from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import financial, operational, patients, pipeline, data_entry, infrastructure, chat, monitoring

app = FastAPI(title="Healthcare Analytics API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(financial.router,      prefix="/api/financial",      tags=["Financial"])
app.include_router(operational.router,    prefix="/api/operational",    tags=["Operational"])
app.include_router(patients.router,       prefix="/api/patients",       tags=["Patients"])
app.include_router(pipeline.router,       prefix="/api/pipeline",       tags=["Pipeline"])
app.include_router(data_entry.router,     prefix="/api/data-entry",     tags=["DataEntry"])
app.include_router(infrastructure.router, prefix="/api/infrastructure", tags=["Infrastructure"])
app.include_router(chat.router,           prefix="/api/chat",           tags=["Chat"])
app.include_router(monitoring.router,     prefix="/api/monitoring",     tags=["Monitoring"])


@app.get("/health")
def health():
    return {"status": "ok"}
