from fastapi import APIRouter
from fastapi.responses import JSONResponse
import traceback
from config import TOKEN_BAJAS
from services.utils import consulta_cr, log_print
from infra.bigquery import cargar_a_bigquery, obtener_ids_exitosos
from models.schemas import ResultadoCargasBajasRequest
import numpy as np
import pandas as pd

router = APIRouter()


@router.get("/dt/bajas/cargar")
def get_bajas_cargar():
    logs = []
    try:
        log_print(logs, "Extrayendo datos de bajas...")
        data = consulta_cr(TOKEN_BAJAS)
        data = data.rename(columns={
            "descripcion_causal": "causal",
            "causal_finiquito": "comentario",
            "fechafiniquito": "fecharetiro",
            "afc": "devolucion_afc",
        })

        # Excluir registros ya cargados exitosamente (según tabla de resultados de bajas)
        try:
            ids_exitosos = obtener_ids_exitosos(tabla="worldwide-470917.cargas_recursiva.resultado_cargas_bajas")
            log_print(logs, f"IDs exitosos (bajas) obtenidos: {len(ids_exitosos)}")
            if ids_exitosos:
                try:
                    _ej = list(ids_exitosos)[:5]
                    log_print(logs, f"Ejemplos IDs exitosos (BQ bajas): {_ej}")
                except Exception:
                    pass
            if ids_exitosos and "rut" in data.columns and "fecharetiro" in data.columns:
                # Normalización robusta de fecha
                raw = data["fecharetiro"].astype(str).str.strip()
                raw = raw.str.replace("/", "-", regex=False).str.slice(0, 10)
                fecha1 = pd.to_datetime(raw, errors="coerce")
                fecha2 = pd.to_datetime(raw, errors="coerce", dayfirst=True)
                fecha_norm = fecha1.fillna(fecha2)
                fecha_str = fecha_norm.dt.strftime("%Y-%m-%d")
                try:
                    _nat = int(fecha_norm.isna().sum())
                    log_print(logs, f"Fechas no parseadas (NaT) en fecharetiro: {_nat}")
                    _ej_raw = raw.head(5).tolist()
                    log_print(logs, f"Ejemplos fechas origen (bajas): {_ej_raw}")
                    _ej_fmt = fecha_str.head(5).tolist()
                    log_print(logs, f"Ejemplos fechas normalizadas (bajas): {_ej_fmt}")
                except Exception:
                    pass
                rut_norm = data["rut"].astype(str).str.replace(".", "", regex=False).str.strip()
                ids_candidatos = rut_norm + "_" + fecha_str.fillna("")
                try:
                    _ej2 = ids_candidatos.head(5).tolist()
                    log_print(logs, f"Ejemplos IDs candidatos (bajas): {_ej2}")
                except Exception:
                    pass
                _before = len(data)
                mask = ~ids_candidatos.isin(ids_exitosos)
                data = data[mask]
                _after = len(data)
                log_print(logs, f"Filtrado por exitosos (bajas): removidos {_before - _after} de {_before}")
        except Exception:
            # Si falla la consulta a BQ, continuamos sin filtrar
            log_print(logs, "Advertencia: fallo consulta de exitosos en BQ, no se filtra bajas")
        sample = data.head(1).to_json(orient="records")
        data_json = data.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "sample": sample, "data": data_json, "logs": logs}
    except Exception as e:
        err = f"{type(e).__name__}: {str(e)}"
        return JSONResponse(status_code=500, content={"ok": False, "error": err, "traceback": traceback.format_exc(), "logs": logs})


@router.post("/dt/bajas/resultado")
def post_bajas_resultado(request: ResultadoCargasBajasRequest):
    try:
        if not request.datos or len(request.datos) == 0:
            return JSONResponse(status_code=400, content={"ok": False, "error": "La lista de datos está vacía"})
        # mapear fecha_finiquito -> fecha_contrato para el cargador existente
        datos_dict = []
        for item in request.datos:
            d = item.dict()
            d["fecha_contrato"] = d.pop("fecha_finiquito")
            datos_dict.append(d)
        resultado = cargar_a_bigquery(datos_dict, tabla="worldwide-470917.cargas_recursiva.resultado_cargas_bajas")
        return {"ok": True, "mensaje": "Datos cargados exitosamente a BigQuery", "resultado": resultado}
    except Exception as e:
        err = f"{type(e).__name__}: {str(e)}"
        return JSONResponse(status_code=500, content={"ok": False, "error": err, "traceback": traceback.format_exc()})

