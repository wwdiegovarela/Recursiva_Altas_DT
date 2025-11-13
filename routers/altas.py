from fastapi import APIRouter
from fastapi.responses import JSONResponse
import json
import pandas as pd
import traceback

from config import TOKEN_ALTAS, TOKEN_ALTAS2
from mappings import apply_mappings_to_df, MAPPINGS_ALTAS
from services.utils import log_print, consulta_cr
from infra.bigquery import cargar_a_bigquery, obtener_ids_exitosos
from models.schemas import ResultadoCargasRequest
import numpy as np
import pandas as pd
from datetime import datetime

router = APIRouter()


@router.get("/dt/altas/cargar")
def get_altas():
    logs = []
    log_print(logs, f"TOKEN_ALTAS: {TOKEN_ALTAS}")
    log_print(logs, f"TOKEN_ALTAS2: {TOKEN_ALTAS2}")
    try:
        # Llamada 1
        data = consulta_cr(TOKEN_ALTAS)
        try:
            data = apply_mappings_to_df(data, MAPPINGS_ALTAS, logs)
        except Exception as map_err:
            log_print(logs, f"Advertencia al aplicar diccionarios: {type(map_err).__name__}: {str(map_err)}")

        # Llamada 2
        data_emp = consulta_cr(TOKEN_ALTAS2)
        data_emp = data_emp.rename(columns={
            'RUT-DV': 'RUT_TRABAJADOR',
            'Region1': 'FAENA_REGION',
            'FECHA NACIMIENTO': 'FECHA_NACIMIENTO',
            'Nombres': 'NOMBRES'
        })
        data_emp['APELLIDOS'] = data_emp['Ap. Paterno'] + ' ' + data_emp['Ap. Materno']

        # Ajustes y merge
        for col in ['SEXO', 'FECHA_NACIMIENTO', 'NOMBRES', 'APELLIDOS', 'NACIONALIDAD']:
            if col in data.columns:
                del data[col]
        data = data.merge(data_emp, on=['RUT_TRABAJADOR'], how='left')
        data = data.rename(columns={'EMAIL': 'EMAIL_TRABAJADOR', 'CARGO': 'CARGO_TRABAJADORES', "NRO_DIAS_DIST_JOR": "DIAS_DIST_JOR"})

        data['NOMBRE_EMPRESA'] = "WORLDWIDE FACILITY SECURITY S.A."
        data['RUT_EMPRESA'] = "76195703-1"
        data['RUT_REP_LEG'] = "10283553-0"
        data['MAIL_REP_LEG'] = "rodrigo.barcelo@wfsa.cl"
        data['FONO_REP_LEG'] = "942672340"
        data['DOMICILIO_REP_LEG'] = "Sexta Avenida 1236 - SAN MIGUEL"

        data['CAMBIO_DOMICILIO'] = "No"
        data['NOTIFICACION_DISCAPACIDAD'] = ""
        if 'DECLARACION_DISCAPACIDAD' in data.columns and 'FECHA_SUSCRPCION' in data.columns:
            data['NOTIFICACION_DISCAPACIDAD'].loc[data['DECLARACION_DISCAPACIDAD'] == "1"] = data['FECHA_SUSCRPCION']
        data['NOTIFICACION_INVALIDEZ'] = ""
        if 'DECLARACION_INVALIDEZ' in data.columns and 'FECHA_SUSCRPCION' in data.columns:
            data['NOTIFICACION_INVALIDEZ'].loc[data['DECLARACION_INVALIDEZ'] == "1"] = data['FECHA_SUSCRPCION']

        try:
            if 'MONTO_2' in data.columns:
                data['MONTO_2'] = pd.to_numeric(data['MONTO_2'], errors='coerce')
            if 'MONTO_3' in data.columns:
                data['MONTO_3'] = pd.to_numeric(data['MONTO_3'], errors='coerce')
            data['MONTO_NO_IMPONIBLE'] = data.get('MONTO_2', 0).fillna(0) + data.get('MONTO_3', 0).fillna(0)
        except Exception:
            pass
        data['REM_Y_ASIGNACIONES'] = "PAGO MENSUAL POR TRANSFERENCIA, GRATIFICACION ART. 47 DEL CODIGO DEL TRABAJO"
        data['ARTICULO_38'] = "No"
        if 'CARGO_TRABAJADORES' in data.columns:
            data['ARTICULO_38'].loc[data['CARGO_TRABAJADORES'].str.lower().str.contains('guardia|operador cctv|recepcionista|jefe de grupo')] = "Si"
        data['DISTRIBUCION_JORNADA'] = "Lunes,Martes,Miercoles,Jueves,Viernes"
        if 'CARGO_TRABAJADORES' in data.columns:
            data['DISTRIBUCION_JORNADA'].loc[data['CARGO_TRABAJADORES'].str.lower().str.contains('guardia|operador cctv|recepcionista|jefe de grupo')] = "Lunes,Martes,Miercoles,Jueves,Viernes,Sabado,Domingo"
        data['DURACION_JORNADA'] = 44
        data['OTRAS_ESTIPULACIONES'] = ""

        try:
            _binary_map = {'0': 'No', '1': 'Si', 0: 'No', 1: 'Si'}
            for _col in ['DECLARACION_DISCAPACIDAD', 'DECLARACION_INVALIDEZ', 'EST', 'SUBCONTRATACION']:
                if _col in data.columns:
                    data[_col] = data[_col].map(_binary_map).fillna(data[_col])
        except Exception:
            pass

        columnas = [
            'NOMBRE_EMPRESA', 'RUT_EMPRESA', 'RUT_REP_LEG', 'MAIL_REP_LEG', 'FONO_REP_LEG',
            'DOMICILIO_REP_LEG', 'COMUNA_CELEBRACION', 'FECHA_SUSCRPCION', 'RUT_TRABAJADOR',
            'DNI_TRABAJADOR', 'FECHA_NACIMIENTO', 'NOMBRES', 'APELLIDOS', 'SEXO',
            'NACIONALIDAD', 'EMAIL_TRABAJADOR', 'TELEFONO', 'REGION', 'COMUNA', 'CALLE',
            'NUMERO', 'DPTO', 'CAMBIO_DOMICILIO', 'DECLARACION_DISCAPACIDAD',
            'NOTIFICACION_DISCAPACIDAD', 'DECLARACION_INVALIDEZ', 'NOTIFICACION_INVALIDEZ',
            'CARGO_TRABAJADORES', 'FUNCIONES', 'SUBCONTRATACION', 'RUT_EMPRESA_PRINCIPAL',
            'EST', 'RUT_EMPRESA_USUARIA', 'FAENA_REGION', 'FAENA_COMUNA', 'FAENA_CALLE',
            'FAENA_NUMERO', 'FAENA_DPTO', 'SUELDO_BASE', 'MONTO_IMPONIBLE',
            'MONTO_NO_IMPONIBLE', 'REM_PERIODO_PAGO', 'REM_FORMA_PAGO', 'GRAT_FORMA_PAGO',
            'REM_Y_ASIGNACIONES', 'TIPO_JORNADA', 'ARTICULO_38', 'NRO_RESOLUCION',
            'FECHA_RESOLUCION', 'DURACION_JORNADA', 'TURNOS',
            'DISTRIBUCION_JORNADA', 'DIAS_DIST_JOR', 'OTRAS_ESTIPULACIONES', 'TIPO_CONTRATO',
            'FECHA_INI_RELABORAL', 'FECHA_FIN_RELABORAL'
        ]
        data = data[[col for col in columnas if col in data.columns]]

        # Excluir ya exitosos
        try:
            ids_exitosos = obtener_ids_exitosos()
            if ids_exitosos:
                contract_date_col = 'FECHA_SUSCRPCION' if 'FECHA_SUSCRPCION' in data.columns else ('FECHA_INI_RELABORAL' if 'FECHA_INI_RELABORAL' in data.columns else None)
                if contract_date_col and 'RUT_TRABAJADOR' in data.columns:
                    fecha1 = pd.to_datetime(data[contract_date_col], format='%Y-%m-%d', errors='coerce')
                    fecha2 = pd.to_datetime(data[contract_date_col], format='%d-%m-%Y', errors='coerce')
                    fecha_norm = fecha1.fillna(fecha2)
                    fecha_str = fecha_norm.dt.strftime('%Y-%m-%d')
                    ids_candidatos = data['RUT_TRABAJADOR'].astype(str) + '_' + fecha_str.fillna('')
                    mask = ~ids_candidatos.isin(ids_exitosos)
                    data = data[mask]
        except Exception as _:
            pass

        data_final_json = data.replace({np.nan: None}).to_dict(orient="records")
        sample = data.head(1).to_json(orient="records")
        return {"ok": True, "sample": sample, "data": data_final_json}
    except Exception as e:
        err = f"{type(e).__name__}: {str(e)}"
        log_print(logs, f"❌ Error en proceso: {err}")
        log_print(logs, f"❌ Stack trace: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"ok": False, "error": err, "logs": logs})


@router.post("/dt/altas/resultado")
def post_resultado_altas(request: ResultadoCargasRequest):
    try:
        if not request.datos or len(request.datos) == 0:
            return JSONResponse(status_code=400, content={"ok": False, "error": "La lista de datos está vacía"})
        datos_dict = [item.dict() for item in request.datos]
        resultado = cargar_a_bigquery(datos_dict)
        return {"ok": True, "mensaje": "Datos cargados exitosamente a BigQuery", "resultado": resultado}
    except Exception as e:
        err = f"{type(e).__name__}: {str(e)}"
        return JSONResponse(status_code=500, content={"ok": False, "error": err, "traceback": traceback.format_exc()})


# ========== Placeholders en construcción ==========

@router.get("/dt/bajas")
def get_bajas():
    return JSONResponse(status_code=501, content={"ok": False, "error": "Endpoint /dt/bajas en construcción"})


@router.get("/dt/anexo")
def get_anexo():
    return JSONResponse(status_code=501, content={"ok": False, "error": "Endpoint /dt/anexo en construcción"})