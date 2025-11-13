from fastapi import APIRouter
from fastapi.responses import JSONResponse
import traceback

from models.schemas import ResultadoClientesRequest
from infra.bigquery import cargar_resultados_clientes

router = APIRouter(prefix="/resultados")


@router.post("/clientes")
def post_resultados_clientes(request: ResultadoClientesRequest):
    try:
        if not request.datos or len(request.datos) == 0:
            return JSONResponse(status_code=400, content={"ok": False, "error": "La lista de datos está vacía"})
        datos_dict = [item.dict() for item in request.datos]
        resultado = cargar_resultados_clientes(datos_dict)
        return {"ok": True, "mensaje": "Resultados cargados exitosamente", "resultado": resultado}
    except Exception as e:
        err = f"{type(e).__name__}: {str(e)}"
        return JSONResponse(status_code=500, content={"ok": False, "error": err, "traceback": traceback.format_exc()})


