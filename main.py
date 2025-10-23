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
from datetime import datetime, timedelta
from mappings import apply_mappings_to_df, MAPPINGS_ALTAS

app = FastAPI(title="WFSA - Proceso ControlRoll", version="1.0.0")

# === Configuración desde variables de entorno ===
API_LOCAL_URL = os.getenv("API_LOCAL_URL")
TOKEN_ALTAS = os.getenv("TOKEN_ALTAS")
TOKEN_ALTAS2 = os.getenv("TOKEN_ALTAS2")
TOKEN_DOC_FIRMA = os.getenv("TOKEN_DOC_FIRMA_WALMART")
TOKEN_DOC_CARPETA = os.getenv("TOKEN_DOC_CARPETA_WALMART")
TOKEN_ASISTENCIA = os.getenv("TOKEN_ASISTENCIA_WALMART")
TOKEN_TRANSFERENCIAS = os.getenv("TOKEN_ASISTENCIA_WALMART")

def log_print(logs, msg):
    print(msg)
    logs.append(str(msg))

# ========== FUNCIONES DE UTILIDAD ==========

def traducir_mes_en_espanol(texto):
    """
    Traduce un texto con formato 'Month YYYY' de inglés a español.
    Ejemplo: 'September 2025' -> 'Septiembre 2025'
    """
    meses = {
        "January": "Enero",
        "February": "Febrero",
        "March": "Marzo",
        "April": "Abril",
        "May": "Mayo",
        "June": "Junio",
        "July": "Julio",
        "August": "Agosto",
        "September": "Septiembre",
        "October": "Octubre",
        "November": "Noviembre",
        "December": "Diciembre"
    }
    
    if pd.isna(texto):
        return texto

    for en, es in meses.items():
        if texto.startswith(en):
            return texto.replace(en, es)
    return texto

def agregar_nombre_subcontrataley_walmart(df, columna_instalacion):
    """Agrega la columna instalacion_subcontrataley mapeando instalaciones"""
    mapa_instalaciones = {
        "LIDER PUENTE ALTO (JOSE LUIS COO) LOCAL 208": "Express 400_208_Plaza P.Alto Y Jose Luis Coo",
        "LIDER PUENTE ALTO (MALEBRAN) LOCAL 280": "Express 400_280_Ciudad Del Sol",
        "LIDER PUENTE ALTO (DIEGO PORTALES) LOCAL 613": "Express_613_Ciudad Del Este",
        "LIDER LA FLORIDA (SANCHEZ FONTECILLA) LOCAL 611": "Express_611_Rojas Magallanes",
        "LIDER C DE LOS VALLE PUDAHUEL (Local 963)": "Express_963_Ciudad De Los Valles",
        "LIDER MACUL (FROILAN ROA) LOCAL 498": "Express 400_498_Froilán Roa",
        "LIDER GRECIA ÑUÑOA (LOCAL 52)": "Express_52_Grecia",
        "LIDER PLAZA LOS DOMINICOS LAS CONDES (LOCAL 624)": "Express_624_La Plaza",
        "LIDER QUINTA NORMAL (CARRASCAL) LOCAL 233": "Express 400_233_Carrascal",
        "LIDER QUILICURA (LOCAL 248)": "Express_248_Quilicura",
        "LIDER LAS REJAS (local 140)": "Express_140_Las Rejas",
        "LIDER MARCOLETA (LOCAL 671)": "Lider_671_Marcoleta",
        "LIDER LA CALERA (LOCAL 983)": "Lider_983_La Calera",
        "LIDER PEÑAFLOR (LOCAL 736)": "Lider_736_Peñaflor",
        "LIDER CONCEPCION (LOCAL 89)": "Lider_89_Biobío",
        "LIDER CONCEPCION (LOCAL 98)": "Lider_98_Concepción",
        "DISPONIBLES WALMART": "Apoyo_Seguridad_Walmart",
        "ACUENTA ANDALIEN CONCEPCION LOCAL 505": "SBA_505_Andalien",
        "ACUENTA VALDIVIA (LOCAL 522)": "SBA_522_Valdivia Terminal",
        "ACUENTA MARIQUINA LOCAL 948": "SBA_948_Mariquina (Los Rios)",
        "ACUENTA PICARTE (LOCAL 559)": "SBA_559_Picarte Valdivia",
        "ACUENTA FUNDADORES (LOCAL 558)": "SBA_558_Fundadores"
    }
    df["instalacion_subcontrataley"] = df[columna_instalacion].map(mapa_instalaciones)
    return df

def intervalo_fechas():
    """Retorna el primer y último día del mes anterior"""
    hoy = datetime.today()
    primer_dia_mes_actual = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    fecha_hasta = primer_dia_mes_actual - timedelta(seconds=1)
    fecha_desde = fecha_hasta.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return fecha_desde, fecha_hasta

def consulta_cr(token):
    """Consulta la API de ControlRoll y retorna un DataFrame normalizado"""
    headers = {
        "method": "report",
        "token": token
    }
    response = requests.get(API_LOCAL_URL, headers=headers, timeout=3600)
    response.raise_for_status()
    
    data_text = response.text
    
    try:
        data_json = json.loads(data_text)
    except json.JSONDecodeError as e:
        print(f"Error parseando JSON. Status code: {response.status_code}")
        print(f"Primeros 500 caracteres: {data_text[:500]}")
        print(f"Content-Type: {response.headers.get('Content-Type', 'N/A')}")
        raise ValueError(f"La API no devolvió JSON válido. Status: {response.status_code}. Primeros 200 chars: {data_text[:200]}") from e
    
    if not isinstance(data_json, list):
        if isinstance(data_json, dict):
            if 'error' in data_json or 'message' in data_json:
                raise ValueError(f"La API devolvió un error: {data_json}")
            data_json = [data_json]
        else:
            raise ValueError(f"La API devolvió un tipo inesperado: {type(data_json)}")
    
    if len(data_json) == 0:
        return pd.DataFrame()
    
    data = pd.DataFrame(data_json)
    
    data.columns = data.columns.str.lower()
    data.columns = data.columns.str.replace(' ', '_')
    data.columns = data.columns.str.replace('.', '')
    data.columns = data.columns.str.replace('%', '')
    data.columns = data.columns.str.replace('-', '_')
    data.columns = data.columns.str.replace('(', '')
    data.columns = data.columns.str.replace(')', '')
    data.columns = data.columns.str.replace('á', 'a')
    data.columns = data.columns.str.replace('é', 'e')
    data.columns = data.columns.str.replace('í', 'i')
    data.columns = data.columns.str.replace('ó', 'o')
    data.columns = data.columns.str.replace('ú', 'u')
    data.columns = data.columns.str.replace('ñ', 'n')
    data.columns = data.columns.str.replace('°', '')
    return data

def get_mantenedor():
    """Retorna el DataFrame de mantenedor de documentos"""
    mantenedor_data = [
        {"Documentos_subcontrataley": "Certificado Curso OS10", "Tipo_Flujo": "1", "modulo": "RRHH", "tablero": "Carpeta", "documento_cr_carpeta": "OS10", "documento_cr_carpeta2": "Certificado Curso"},
        {"Documentos_subcontrataley": "Liquidaciones de Sueldo", "modulo": "REMUNERACIONES", "tablero": "Liquidaciones", "tablero2": "Por Centro de Costo"},
        {"Documentos_subcontrataley": "Toma de Conoc. de Trab. Información de Riesgos Laborales", "Tipo_Flujo": "2", "modulo": "RRHH", "tablero": "Formatos", "documento_cr_carpeta": "Documentos Por Firmar", "documento_cr_carpeta2": "KIT PREVENCION DE RIESGOS"},
        {"Documentos_subcontrataley": "Contratos de Trabajo", "Tipo_Flujo": "2", "modulo": "RRHH", "tablero": "Formatos", "documento_cr_carpeta": "Documentos Por Firmar", "documento_cr_carpeta2": "Contrato"},
        {"Documentos_subcontrataley": "Toma Conoc. Trab. Procedimiento de Trabajo Seguro", "Tipo_Flujo": "2", "modulo": "RRHH", "tablero": "Formatos", "documento_cr_carpeta": "Documentos Por Firmar", "documento_cr_carpeta2": "KIT PREVENCION DE RIESGOS"},
        {"Documentos_subcontrataley": "Registro Difusión Trabajador Reglamento Interno", "Tipo_Flujo": "2", "modulo": "RRHH", "tablero": "Formatos", "documento_cr_carpeta": "Documentos Por Firmar", "documento_cr_carpeta2": "Contrato"},
        {"Documentos_subcontrataley": "Toma Conoc. Trab. Matriz IPER del Contratista", "Tipo_Flujo": "2", "modulo": "RRHH", "tablero": "Formatos", "documento_cr_carpeta": "Documentos Por Firmar", "documento_cr_carpeta2": "KIT PREVENCION DE RIESGOS"},
        {"Documentos_subcontrataley": "Toma Conoc. Trab. Programa de Trabajo Preventivo", "Tipo_Flujo": "2", "modulo": "RRHH", "tablero": "Formatos", "documento_cr_carpeta": "Documentos Por Firmar", "documento_cr_carpeta2": "KIT PREVENCION DE RIESGOS"},
        {"Documentos_subcontrataley": "Capacitación Uso y Mantención de EPP", "Tipo_Flujo": "1", "modulo": "RRHH", "tablero": "Carpeta", "documento_cr_carpeta": "CONTRACTUAL", "documento_cr_carpeta2": "Capacitacion Uso EPP"},
        {"Documentos_subcontrataley": "Capacitación para Pers. Trab. en Prevención de Riesgos Laborales", "Tipo_Flujo": "2", "modulo": "RRHH", "tablero": "Formatos", "documento_cr_carpeta": "Documentos Por Firmar", "documento_cr_carpeta2": "KIT PREVENCION DE RIESGOS"},
        {"Documentos_subcontrataley": "Carta Aviso Despido o Renuncia", "Tipo_Flujo": "1", "modulo": "RRHH", "tablero": "Carpeta", "documento_cr_carpeta": "CONTRACTUAL", "documento_cr_carpeta2": "CARTAS DESPIDO"},
        {"Documentos_subcontrataley": "Carta Aviso Despido o Renuncia", "Tipo_Flujo": "1", "modulo": "RRHH", "tablero": "Carpeta", "documento_cr_carpeta": "CONTRACTUAL", "documento_cr_carpeta2": "RENUNCIA VOLUNTARIA"},
        {"Documentos_subcontrataley": "Finiquitos de Contrato", "Tipo_Flujo": "1", "modulo": "RRHH", "tablero": "Carpeta", "documento_cr_carpeta": "CONTRACTUAL", "documento_cr_carpeta2": "Finiquito"},
        {"Documentos_subcontrataley": "Anexo Traslado", "Tipo_Flujo": "2", "modulo": "RRHH", "tablero": "Formatos", "documento_cr_carpeta": "Documentos Por Firmar", "documento_cr_carpeta2": "AnexoTraslado"},
        {"Documentos_subcontrataley": "Liquidaciones de Sueldo", "modulo": "COMERCIAL", "tablero": "Carpeta Instalación", "documento_cr_carpeta": "TRANSFERENCIA"},
        {"Documentos_subcontrataley": "Entrega EPP", "Tipo_Flujo": "1", "modulo": "RRHH", "tablero": "Carpeta", "documento_cr_carpeta": "CONTRACTUAL", "documento_cr_carpeta2": "Entrega de EPP"},
        {"Documentos_subcontrataley": "Cédulas de Identidad", "Tipo_Flujo": "1", "modulo": "RRHH", "tablero": "Carpeta", "documento_cr_carpeta": "CONTRACTUAL", "documento_cr_carpeta2": "Cedula Identidad"},
        {"Documentos_subcontrataley": "Certificado de Antecedentes", "Tipo_Flujo": "1", "modulo": "RRHH", "tablero": "Carpeta", "documento_cr_carpeta": "CONTRACTUAL", "documento_cr_carpeta2": "Certificado Antecedentes"},
        {"Documentos_subcontrataley": "Libro de Asistencia", "Tipo_Flujo": "3", "modulo": "OPERACIONES", "tablero": "FaceID", "tablero2": "Reportería", "tablero3": "Libro Asistencia por Colaborador", "documento_cr_carpeta": "Asistencia"},
        {"Documentos_subcontrataley": "Recepción Reglamento Especial de Empresas Contratistas (Trabajador)", "Tipo_Flujo": "2", "modulo": "RRHH", "tablero": "Formatos", "documento_cr_carpeta": "Documentos Por Firmar", "documento_cr_carpeta2": "KIT PREVENCION DE RIESGOS"}
    ]
    return pd.DataFrame(mantenedor_data)

# ========== ENDPOINTS DE LA API ==========

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/subcontrataley/walmart/firmas")
def get_firmas():
    """
    Obtiene datos de firmas del mes anterior
    GET /dt/firmas
    """
    try:
        fecha_desde, fecha_hasta = intervalo_fechas()
        data_firma = consulta_cr(TOKEN_DOC_FIRMA)
        data_firma['flog'] = pd.to_datetime(data_firma['flog'], format='%Y-%m-%d %H:%M:%S')
        data_firma = data_firma.loc[(data_firma.flog >= fecha_desde) & (data_firma.flog <= fecha_hasta)]
        data_firma = data_firma.loc[data_firma.firma_del_colaborador == "Firmado Colaborador"][['rut', 'nombre_del_documento', 'tipo_del_documento', 'flog']]
        data_firma["tipo_del_documento"].loc[(data_firma.tipo_del_documento == "AnexoPersonalizado") & (data_firma.nombre_del_documento == "KIT PREVENCION DE RIESGOS")] = "KIT PREVENCION DE RIESGOS"
        data_firma = data_firma.merge(get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta", "documento_cr_carpeta2"]], left_on="tipo_del_documento", right_on="documento_cr_carpeta2", how="left")
        data_firma = data_firma[["rut", "modulo", "tablero", "documento_cr_carpeta", "tipo_del_documento", "nombre_del_documento", "Documentos_subcontrataley", "flog"]]
        
        result = data_firma.replace({np.nan: None}).to_dict(orient="records")
        return {
            "ok": True,
            "periodo": {"desde": fecha_desde.isoformat(), "hasta": fecha_hasta.isoformat()},
            "total_registros": len(result),
            "data": result
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()}
        )

@app.get("/subcontrataley/walmart/carpeta")
def get_carpeta():
    """
    Obtiene datos normalizados de documentos del mes anterior
    GET /dt/documentos
    """
    try:
        fecha_desde, fecha_hasta = intervalo_fechas()
        data_norm = consulta_cr(TOKEN_DOC_CARPETA)
        data_norm['flog'] = pd.to_datetime(data_norm['flog'], format='%Y-%m-%d %H:%M:%S')
        data_norm = data_norm.loc[(data_norm.flog >= fecha_desde) & (data_norm.flog <= fecha_hasta)]
        data_norm = data_norm[["rut", "tipo_documento", "nombre_documento", "flog"]]
        data_norm = data_norm.merge(get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta", "documento_cr_carpeta2"]], left_on="tipo_documento", right_on="documento_cr_carpeta2", how="left")
        data_norm = data_norm[["rut", "modulo", "tablero", "documento_cr_carpeta", "tipo_documento", "nombre_documento", "Documentos_subcontrataley", "flog"]]
        
        result = data_norm.replace({np.nan: None}).to_dict(orient="records")
        return {
            "ok": True,
            "periodo": {"desde": fecha_desde.isoformat(), "hasta": fecha_hasta.isoformat()},
            "total_registros": len(result),
            "data": result
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()}
        )

@app.get("/subcontrataley/walmart/asistencia")
def get_asistencia():
    """
    Obtiene datos de asistencia (FaceID enrolados)
    GET /dt/asistencia
    """
    try:
        data_asistencia = consulta_cr(TOKEN_ASISTENCIA)
        data_asistencia = data_asistencia.loc[data_asistencia['faceid_enrolado'] == "SI"]
        fecha_desde, fecha_hasta = intervalo_fechas()
        data_asistencia = data_asistencia[["rut", "cliente", "instalacion", "cecos"]]
        data_asistencia['tipo_documento'] = 'Asistencia'
        data_asistencia = data_asistencia.merge(get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta"]], left_on="tipo_documento", right_on="documento_cr_carpeta", how="left")
        data_asistencia['Desde'] = fecha_desde.strftime('%d-%m-%Y')
        data_asistencia['Hasta'] = fecha_hasta.strftime('%d-%m-%Y')
        data_asistencia = agregar_nombre_subcontrataley_walmart(data_asistencia, columna_instalacion="instalacion")
        
        result = data_asistencia.replace({np.nan: None}).to_dict(orient="records")
        return {
            "ok": True,
            "periodo": {"desde": fecha_desde.strftime('%d-%m-%Y'), "hasta": fecha_hasta.strftime('%d-%m-%Y')},
            "total_registros": len(result),
            "data": result
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()}
        )

@app.get("/subcontrataley/walmart/liquidaciones")
def get_liquidaciones():
    """
    Obtiene datos de liquidaciones por instalación
    GET /dt/liquidaciones
    """
    try:
        data_liquidaciones = consulta_cr(TOKEN_ASISTENCIA)
        data_liquidaciones = data_liquidaciones[["cliente", "instalacion", "cecos"]].drop_duplicates()
        data_liquidaciones['tipo_documento'] = 'Liquidaciones'
        data_liquidaciones = data_liquidaciones.merge(get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "tablero2"]], left_on="tipo_documento", right_on="tablero", how="left")
        data_liquidaciones = data_liquidaciones[["instalacion", "cecos", "modulo", "tablero", "Documentos_subcontrataley"]]
        fecha_desde, fecha_hasta = intervalo_fechas()
        data_liquidaciones['periodo'] = fecha_hasta
        data_liquidaciones['periodo'] = data_liquidaciones['periodo'].dt.strftime('%B %Y')
        data_liquidaciones['periodo'] = data_liquidaciones['periodo'].apply(traducir_mes_en_espanol)
        data_liquidaciones = agregar_nombre_subcontrataley_walmart(data_liquidaciones, columna_instalacion="instalacion")
        
        result = data_liquidaciones.replace({np.nan: None}).to_dict(orient="records")
        
        return {
            "ok": True,
            "periodo": fecha_hasta.strftime('%B %Y'),
            "total_registros": len(result),
            "data": result
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()}
        )

@app.get("/subcontrataley/walmart/transferencias")
def get_transferencias():
    """
    Obtiene datos de transferencias
    GET /dt/transferencias
    """
    try:
        data_transferencias = consulta_cr(TOKEN_TRANSFERENCIAS)
        data_transferencias['tipo_documento'] = 'TRANSFERENCIA'
        data_transferencias = data_transferencias.merge(get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "tablero2", "tablero3", "documento_cr_carpeta"]], left_on="tipo_documento", right_on="documento_cr_carpeta", how="left")
        data_transferencias = data_transferencias[["instalacion", "codcecoscr", "modulo", "tablero", "tipo_documento", "nombre_archivo", "Documentos_subcontrataley", "flog"]]
        data_transferencias = agregar_nombre_subcontrataley_walmart(data_transferencias, columna_instalacion="instalacion")
        
        result = data_transferencias.replace({np.nan: None}).to_dict(orient="records")
        result_json=data_transferencias.replace({np.nan: None}).to_dict(orient="records")
        return {
            "ok": True,
            "total_registros": len(result_json),
            "data": result_json
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()}
        )

@app.get("/dt/altas")
def get_altas():
    """
    Ejecuta el flujo original sin alterar el orden de operaciones ni la lógica.
    Devuelve logs y un resumen del resultado.
    """

    logs = []
    log_print(logs, f"TOKEN_ALTAS: {TOKEN_ALTAS}")
    log_print(logs, f"TOKEN_ALTAS2: {TOKEN_ALTAS2}")
    try:
        # ========= BLOQUE 1: Llamada a la API con TOKEN =========
        log_print(logs, "=== OBTENIENDO Y PROCESANDO DATOS ===")

        # Preparar parámetros para la API local
        headers = {
            "method": "report",
            "token": TOKEN_ALTAS
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
        # Aplicación de diccionarios justo después de crear el DataFrame
        try:
            data = apply_mappings_to_df(data, MAPPINGS_ALTAS, logs)
        except Exception as map_err:
            log_print(logs, f"Advertencia al aplicar diccionarios: {type(map_err).__name__}: {str(map_err)}")

        # ========= BLOQUE 2: Segunda llamada a la API con TOKEN2 =========
        headers = {
            "method": "report",
            "token": TOKEN_ALTAS2
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
        # Suma numérica segura para MONTO_NO_IMPONIBLE
        try:
            if 'MONTO_2' in data.columns:
                data['MONTO_2'] = pd.to_numeric(data['MONTO_2'], errors='coerce')
            if 'MONTO_3' in data.columns:
                data['MONTO_3'] = pd.to_numeric(data['MONTO_3'], errors='coerce')
            data['MONTO_NO_IMPONIBLE'] = data.get('MONTO_2', 0).fillna(0) + data.get('MONTO_3', 0).fillna(0)
        except Exception as _e:
            log_print(logs, f"Advertencia al convertir/sumar montos: {type(_e).__name__}: {str(_e)}")
        data['REM_Y_ASIGNACIONES'] ="PAGO MENSUAL POR TRANSFERENCIA, GRATIFICACION ART. 47 DEL CODIGO DEL TRABAJO"
        data['ARTICULO_38'] ="No"
        data['ARTICULO_38'].loc[data['CARGO_TRABAJADORES'].str.lower().str.contains('guardia|operador cctv|recepcionista|jefe de grupo')]="Si"

        data['DISTRIBUCION_JORNADA'] ="Lunes,Martes,Miercoles,Jueves,Viernes"
        data['DISTRIBUCION_JORNADA'].loc[data['CARGO_TRABAJADORES'].str.lower().str.contains('guardia|operador cctv|recepcionista|jefe de grupo')]="Lunes,Martes,Miercoles,Jueves,Viernes,Sabado,Domingo"


        data['OTRAS_ESTIPULACIONES'] =""


        # Mapeo binario 0/1 -> "No"/"Si" para campos solicitados
        try:
            _binary_map = {'0': 'No', '1': 'Si', 0: 'No', 1: 'Si'}
            for _col in ['DECLARACION_DISCAPACIDAD', 'DECLARACION_INVALIDEZ', 'EST', 'SUBCONTRATACION']:
                if _col in data.columns:
                    data[_col] = data[_col].map(_binary_map).fillna(data[_col])
        except Exception as _e:
            log_print(logs, f"Advertencia al mapear binarios: {type(_e).__name__}: {str(_e)}")


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
            "sample": sample,
            "data": data_final_json
        }

    except Exception as e:
        err = f"{type(e).__name__}: {str(e)}"
        log_print(logs, f"❌ Error en proceso: {err}")
        log_print(logs, f"❌ Stack trace: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"ok": False, "error": err, "logs": logs})



# Ejecutar con: uvicorn main:app --reload --host 0.0.0.0 --port 8000





