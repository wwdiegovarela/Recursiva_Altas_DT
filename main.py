from fastapi import FastAPI

from routers.system import router as system_router
from routers.altas import router as altas_router
from routers.bajas import router as bajas_router
from routers.anexo import router as anexo_router
from routers.certificadoras import router as certificadoras_router


app = FastAPI(title="WFSA - Proceso ControlRoll", version="1.0.0")

# Registrar routers
app.include_router(system_router)
app.include_router(altas_router)
app.include_router(bajas_router)
app.include_router(anexo_router)
app.include_router(certificadoras_router)

# Ejecutar con: uvicorn main:app --reload --host 0.0.0.0 --port 8000


