# main.py
# -*- coding: utf-8 -*-
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import requests
import pandas as pd
import json
import os
import traceback
import numpy as np
app = FastAPI(title="WFSA - Proceso ControlRoll", version="1.0.0")

# === Configuración desde variables de entorno (igual que el flujo original) ===
API_LOCAL_URL = os.getenv("API_LOCAL_URL")
TOKEN = os.getenv("TOKEN")
TOKEN2 = os.getenv("TOKEN2")

def log_print(logs, msg):
    print(msg)
    logs.append(str(msg))

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/process")
def process():
    """
    Ejecuta el flujo original sin alterar el orden de operaciones ni la lógica.
    Devuelve logs y un resumen del resultado.
    """
    log_print(logs, f"TOKEN: {TOKEN}")
    log_print(logs, f"TOKEN2: {TOKEN2}")
    logs = []
    try:
        # ========= BLOQUE 1: Llamada a la API con TOKEN =========
        log_print(logs, "=== OBTENIENDO Y PROCESANDO DATOS ===")

        # Preparar parámetros para la API local
        headers = {
            "method": "report",
            "token": TOKEN
        }
        log_print(logs, f"API URL: {API_LOCAL_URL}")
        log_print(logs, f"Headers: {headers}")

        try:
            log_print(logs, "🔄 Iniciando llamada a ControlRoll...")
            response = requests.get(API_LOCAL_URL, headers=headers, timeout=3600)
            log_print(logs, "✅ Llamada completada")
        except requests.exceptions.Timeout:
            error_msg = "Timeout: La API externa tardó más de 1 hora en responder"
            log_print(logs, f"❌ {error_msg}")
            return JSONResponse(status_code=504, content={"ok": False, "error": error_msg, "logs": logs})
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Error de conexión con la API externa: {str(e)}"
            log_print(logs, f"❌ {error_msg}")
            return JSONResponse(status_code=502, content={"ok": False, "error": error_msg, "logs": logs})
        except requests.exceptions.RequestException as e:
            error_msg = f"Error en la petición HTTP: {str(e)}"
            log_print(logs, f"❌ {error_msg}")
            return JSONResponse(status_code=500, content={"ok": False, "error": error_msg, "logs": logs})
        except Exception as e:
            error_msg = f"Error inesperado: {type(e).__name__}: {str(e)}"
            log_print(logs, f"❌ {error_msg}")
            log_print(logs, f"❌ Stack trace: {traceback.format_exc()}")
            return JSONResponse(status_code=500, content={"ok": False, "error": error_msg, "logs": logs})

        log_print(logs, f"Status code: {response.status_code}")
        response.raise_for_status()
        data_text = response.text
        log_print(logs, f"Longitud de respuesta: {len(data_text)}")
        log_print(logs, f"Primeros 200 caracteres: {data_text[:200]}")

        data_json = json.loads(data_text)
        log_print(logs, f"Datos obtenidos: {len(data_json)} registros")
        log_print(logs, f"Primer registro: {data_json[0] if data_json else 'No hay datos'}")

        if not data_json:
            log_print(logs, "No hay datos para procesar")

        # Convertir a DataFrame (primer dataset)
        data_text = response.text
        log_print(logs, f"Longitud de respuesta: {len(data_text)}")
        log_print(logs, f"Primeros 200 caracteres: {data_text[:200]}")

        data_json = json.loads(data_text)
        data = pd.DataFrame(data_json)

        # ========= BLOQUE 2: Segunda llamada a la API con TOKEN2 =========
        headers = {
            "method": "report",
            "token": TOKEN2
        }
        log_print(logs, f"API URL: {API_LOCAL_URL}")
        log_print(logs, f"Headers: {headers}")

        try:
            log_print(logs, "🔄 Iniciando llamada a ControlRoll...")
            response = requests.get(API_LOCAL_URL, headers=headers, timeout=3600)
            log_print(logs, "✅ Llamada completada")
        except requests.exceptions.Timeout:
            error_msg = "Timeout: La API externa tardó más de 1 hora en responder"
            log_print(logs, f"❌ {error_msg}")
            return JSONResponse(status_code=504, content={"ok": False, "error": error_msg, "logs": logs})
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Error de conexión con la API externa: {str(e)}"
            log_print(logs, f"❌ {error_msg}")
            return JSONResponse(status_code=502, content={"ok": False, "error": error_msg, "logs": logs})
        except requests.exceptions.RequestException as e:
            error_msg = f"Error en la petición HTTP: {str(e)}"
            log_print(logs, f"❌ {error_msg}")
            return JSONResponse(status_code=500, content={"ok": False, "error": error_msg, "logs": logs})
        except Exception as e:
            error_msg = f"Error inesperado: {type(e).__name__}: {str(e)}"
            log_print(logs, f"❌ {error_msg}")
            log_print(logs, f"❌ Stack trace: {traceback.format_exc()}")
            return JSONResponse(status_code=500, content={"ok": False, "error": error_msg, "logs": logs})

        log_print(logs, f"Status code: {response.status_code}")
        response.raise_for_status()
        data_text = response.text
        log_print(logs, f"Longitud de respuesta: {len(data_text)}")
        log_print(logs, f"Primeros 200 caracteres: {data_text[:200]}")

        data_json = json.loads(data_text)
        log_print(logs, f"Datos obtenidos: {len(data_json)} registros")
        log_print(logs, f"Primer registro: {data_json[0] if data_json else 'No hay datos'}")

        if not data_json:
            log_print(logs, "No hay datos para procesar")

        # Convertir a DataFrame (segundo dataset)
        data_text = response.text
        log_print(logs, f"Longitud de respuesta: {len(data_text)}")
        log_print(logs, f"Primeros 200 caracteres: {data_text[:200]}")

        data_json = json.loads(data_text)
        data_emp = pd.DataFrame(data_json)

        # Renombres/derivaciones (igual al flujo original)
        data_emp = data_emp.rename(columns={
            'RUT-DV': 'RUT_TRABAJADOR',
            'Region1': 'FAENA_REGION',
            'FECHA NACIMIENTO': 'FECHA_NACIMIENTO',
            'Nombres': 'NOMBRES'
        })
        data_emp['APELLIDOS'] = data_emp['Ap. Paterno'] + ' ' + data_emp['Ap. Materno']

        # Eliminaciones en 'data' (igual al flujo original; puede lanzar KeyError si no existen)
        del data['SEXO']
        del data['FECHA_NACIMIENTO']
        del data['NOMBRES']
        del data['APELLIDOS']
        del data['NACIONALIDAD']

        data = data.merge(data_emp,on=['RUT_TRABAJADOR'], how='left')
        data=data.rename(columns={'EMAIL':'EMAIL_TRABAJADOR','CARGO':'CARGO_TRABAJADORES',"NRO_DIAS_DIST_JOR":"DIAS_DIST_JOR"})


        data['NOMBRE_EMPRESA']	="WORLDWIDE FACILITY SECURITY S.A."
        data['RUT_EMPRESA']	="76195703-1"
        data['RUT_REP_LEG']	="10283553-0"
        data['MAIL_REP_LEG']	="rodrigo.barcelo@wfsa.cl"
        data['FONO_REP_LEG']	="942672340"
        data['DOMICILIO_REP_LEG'] ="Sexta Avenida 1236 - SAN MIGUEL"

        data['CAMBIO_DOMICILIO'] ="No"
        data['NOTIFICACION_DISCAPACIDAD']=""
        data['NOTIFICACION_DISCAPACIDAD'].loc[data['DECLARACION_DISCAPACIDAD']=="1"]=data['FECHA_SUSCRPCION']

        data['NOTIFICACION_INVALIDEZ']=""
        data['NOTIFICACION_INVALIDEZ'].loc[data['DECLARACION_INVALIDEZ']=="1"]=data['FECHA_SUSCRPCION']
        data['MONTO_NO_IMPONIBLE'] = data['MONTO_2']+data['MONTO_3']
        data['REM_Y_ASIGNACIONES'] ="PAGO MENSUAL POR TRANSFERENCIA, GRATIFICACION ART. 47 DEL CODIGO DEL TRABAJO"
        data['ARTICULO_38'] ="No"
        data['ARTICULO_38'].loc[data['CARGO_TRABAJADORES'].str.lower().str.contains('guardia|operador cctv|recepcionista|jefe de grupo')]="Si"

        data['DISTRIBUCION_JORNADA'] ="Lunes,Martes,Miercoles,Jueves,Viernes"
        data['DISTRIBUCION_JORNADA'].loc[data['CARGO_TRABAJADORES'].str.lower().str.contains('guardia|operador cctv|recepcionista|jefe de grupo')]="Lunes,Martes,Miercoles,Jueves,Viernes,Sabado,Domingo"


        data['OTRAS_ESTIPULACIONES'] =""


        # Reorden de columnas (exacto al original)
        data = data[[
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
        ]]
        data_final_json=data.replace({np.nan: None}).to_dict(orient="records")
    
        # Resumen de salida
        sample = data.head(1).to_json(orient="records")
        return {
            "ok": True,
            "logs": logs,
            "result_rows": len(data),
            "sample": sample,
            "data": data_final_json
        }

    except Exception as e:
        err = f"{type(e).__name__}: {str(e)}"
        log_print(logs, f"❌ Error en proceso: {err}")
        log_print(logs, f"❌ Stack trace: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"ok": False, "error": err, "logs": logs})



# Ejecutar con: uvicorn main:app --reload --host 0.0.0.0 --port 8000



