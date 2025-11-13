import json
from datetime import datetime, timedelta
from typing import Tuple

import numpy as np
import pandas as pd
import requests

from config import API_LOCAL_URL


def log_print(logs, msg):
    print(msg)
    logs.append(str(msg))


def traducir_mes_en_espanol(texto: str) -> str:
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
        "December": "Diciembre",
    }
    if pd.isna(texto):
        return texto
    for en, es in meses.items():
        if str(texto).startswith(en):
            return str(texto).replace(en, es)
    return texto


def agregar_nombre_subcontrataley_walmart(df: pd.DataFrame, columna_instalacion: str) -> pd.DataFrame:
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
        "ACUENTA FUNDADORES (LOCAL 558)": "SBA_558_Fundadores",
    }
    df["instalacion_subcontrataley"] = df[columna_instalacion].map(mapa_instalaciones)
    return df


def agregar_nombre_subcontrataley_telefonica(df: pd.DataFrame, columna_instalacion: str) -> pd.DataFrame:
    df["instalacion_subcontrataley"] = df[columna_instalacion]
    return df


def agregar_nombre_subcontrataley_santotomas(df: pd.DataFrame, columna_instalacion: str) -> pd.DataFrame:
    df["instalacion_subcontrataley"] = df[columna_instalacion]
    return df


def agregar_nombre_subcontrataley_indumotora(df: pd.DataFrame, columna_instalacion: str) -> pd.DataFrame:
    df["instalacion_subcontrataley"] = df[columna_instalacion]
    return df


def agregar_nombre_subcontrataley_unimarc(df: pd.DataFrame, columna_instalacion: str) -> pd.DataFrame:
    df["instalacion_subcontrataley"] = df[columna_instalacion]
    return df


def agregar_nombre_subcontrataley_seniorsuites(df: pd.DataFrame, columna_instalacion: str) -> pd.DataFrame:
    df["instalacion_subcontrataley"] = df[columna_instalacion]
    return df

def intervalo_fechas() -> Tuple[datetime, datetime]:
    hoy = datetime.today()
    primer_dia_mes_actual = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    fecha_hasta = primer_dia_mes_actual - timedelta(seconds=1)
    fecha_desde = fecha_hasta.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return fecha_desde, fecha_hasta


def consulta_cr(token: str) -> pd.DataFrame:
    headers = {"method": "report", "token": token}
    response = requests.get(API_LOCAL_URL, headers=headers, timeout=3600)
    response.raise_for_status()

    data_text = response.text
    data_json = json.loads(data_text)
    if not isinstance(data_json, list):
        if isinstance(data_json, dict):
            data_json = [data_json]
        else:
            raise ValueError(f"Respuesta inesperada de la API: {type(data_json)}")
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


def get_mantenedor() -> pd.DataFrame:
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
        {"Documentos_subcontrataley": "Recepción Reglamento Especial de Empresas Contratistas (Trabajador)", "Tipo_Flujo": "2", "modulo": "RRHH", "tablero": "Formatos", "documento_cr_carpeta": "Documentos Por Firmar", "documento_cr_carpeta2": "KIT PREVENCION DE RIESGOS"},
    ]
    return pd.DataFrame(mantenedor_data)

