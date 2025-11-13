from fastapi import APIRouter
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
import traceback

from config import (
    TOKEN_DOC_FIRMA_INDU,
    TOKEN_DOC_CARPETA_INDU,
    TOKEN_ASISTENCIA_INDU,
    TOKEN_TRANSFERENCIAS_INDU,
)
from services.utils import (
    intervalo_fechas,
    consulta_cr,
    get_mantenedor,
    traducir_mes_en_espanol,
    agregar_nombre_subcontrataley_indumotora,
)


router = APIRouter(prefix="/ariba/indumotora")


@router.get("/kpr")
def kpr():
    try:
        desde, hasta = intervalo_fechas()
        df = consulta_cr(TOKEN_DOC_FIRMA_INDU)
        df["flog"] = pd.to_datetime(df["flog"], format="%Y-%m-%d %H:%M:%S")
        df = df.loc[(df.flog >= desde) & (df.flog <= hasta)]
        df = df.loc[df.firma_del_colaborador == "Firmado Colaborador"][["rut", "nombre_del_documento", "tipo_del_documento", "flog"]]
        df["tipo_del_documento"].loc[
            (df.tipo_del_documento == "AnexoPersonalizado") & (df.nombre_del_documento == "KIT PREVENCION DE RIESGOS")
        ] = "KIT PREVENCION DE RIESGOS"
        df = df.loc[df["tipo_del_documento"] == "KIT PREVENCION DE RIESGOS"]
        df = df.merge(
            get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta", "documento_cr_carpeta2"]],
            left_on="tipo_del_documento",
            right_on="documento_cr_carpeta2",
            how="left",
        )
        df = df[["rut", "modulo", "tablero", "documento_cr_carpeta", "tipo_del_documento", "nombre_del_documento", "Documentos_subcontrataley", "flog"]]
        result = df.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/contrato")
def contrato():
    try:
        desde, hasta = intervalo_fechas()
        df = consulta_cr(TOKEN_DOC_FIRMA_INDU)
        df["flog"] = pd.to_datetime(df["flog"], format="%Y-%m-%d %H:%M:%S")
        df = df.loc[(df.flog >= desde) & (df.flog <= hasta)]
        df = df.loc[df.firma_del_colaborador == "Firmado Colaborador"][["rut", "nombre_del_documento", "tipo_del_documento", "flog"]]
        df["tipo_del_documento"].loc[
            (df.tipo_del_documento == "AnexoPersonalizado") & (df.nombre_del_documento == "KIT PREVENCION DE RIESGOS")
        ] = "KIT PREVENCION DE RIESGOS"
        df = df.loc[df["tipo_del_documento"] == "Contrato"]
        df = df.merge(
            get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta", "documento_cr_carpeta2"]],
            left_on="tipo_del_documento",
            right_on="documento_cr_carpeta2",
            how="left",
        )
        df = df[["rut", "modulo", "tablero", "documento_cr_carpeta", "tipo_del_documento", "nombre_del_documento", "Documentos_subcontrataley", "flog"]]
        result = df.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


def _carpeta(tipo, multiple: bool = False):
    desde, hasta = intervalo_fechas()
    df = consulta_cr(TOKEN_DOC_CARPETA_INDU)
    df["flog"] = pd.to_datetime(df["flog"], format="%Y-%m-%d %H:%M:%S")
    df = df.loc[(df.flog >= desde) & (df.flog <= hasta)]
    df = df[["rut", "tipo_documento", "nombre_documento", "flog"]]
    if multiple:
        df = df.loc[df["tipo_documento"].isin(tipo)]
    else:
        df = df.loc[df["tipo_documento"] == tipo]
    df = df.merge(
        get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta", "documento_cr_carpeta2"]],
        left_on="tipo_documento",
        right_on="documento_cr_carpeta2",
        how="left",
    )
    df = df[["rut", "modulo", "tablero", "documento_cr_carpeta", "tipo_documento", "nombre_documento", "Documentos_subcontrataley", "flog"]]
    result = df.replace({np.nan: None}).to_dict(orient="records")
    return desde, hasta, result


@router.get("/finiquito")
def finiquito():
    try:
        desde, hasta, result = _carpeta("Finiquito")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/epp")
def epp():
    try:
        desde, hasta, result = _carpeta("Entrega de EPP")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/os10")
def os10():
    try:
        desde, hasta, result = _carpeta("Certificado Curso")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/antecedentes")
def antecedentes():
    try:
        desde, hasta, result = _carpeta("Certificado Antecedentes")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/cedula")
def cedula():
    try:
        desde, hasta, result = _carpeta("Cedula Identidad")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/cdrv")
def cdrv():
    try:
        desde, hasta, result = _carpeta(["CARTAS DESPIDO", "RENUNCIA VOLUNTARIA"], multiple=True)
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/cuepp")
def cuepp():
    try:
        desde, hasta, result = _carpeta("Capacitacion Uso EPP")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/anexotraslado")
def anexotraslado():
    try:
        desde, hasta, result = _carpeta("AnexoTraslado")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/asistencia")
def asistencia():
    try:
        df = consulta_cr(TOKEN_ASISTENCIA_INDU)
        df = df.loc[df["faceid_enrolado"] == "SI"]
        desde, hasta = intervalo_fechas()
        df = df[["rut", "cliente", "instalacion", "cecos"]]
        df["tipo_documento"] = "Asistencia"
        df = df.merge(
            get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta"]],
            left_on="tipo_documento",
            right_on="documento_cr_carpeta",
            how="left",
        )
        df["Desde"] = desde.strftime("%d-%m-%Y")
        df["Hasta"] = hasta.strftime("%d-%m-%Y")
        df = agregar_nombre_subcontrataley_indumotora(df, columna_instalacion="instalacion")
        result = df.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": desde.strftime("%d-%m-%Y"), "hasta": hasta.strftime("%d-%m-%Y")}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/liquidaciones")
def liquidaciones():
    try:
        df = consulta_cr(TOKEN_ASISTENCIA_INDU)
        df = df[["cliente", "instalacion", "cecos"]].drop_duplicates()
        df["tipo_documento"] = "Liquidaciones"
        df = df.merge(
            get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "tablero2"]],
            left_on="tipo_documento",
            right_on="tablero",
            how="left",
        )
        df = df[["instalacion", "cecos", "modulo", "tablero", "Documentos_subcontrataley"]]
        _, hasta = intervalo_fechas()
        df["periodo"] = hasta
        df["periodo"] = df["periodo"].dt.strftime("%B %Y")
        df["periodo"] = df["periodo"].apply(traducir_mes_en_espanol)
        df = agregar_nombre_subcontrataley_indumotora(df, columna_instalacion="instalacion")
        result = df.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": hasta.strftime("%B %Y"), "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/transferencias")
def transferencias():
    try:
        df = consulta_cr(TOKEN_TRANSFERENCIAS_INDU)
        df["tipo_documento"] = "TRANSFERENCIA"
        df = df.merge(
            get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "tablero2", "tablero3", "documento_cr_carpeta"]],
            left_on="tipo_documento",
            right_on="documento_cr_carpeta",
            how="left",
        )
        df = df[["instalacion", "codcecoscr", "modulo", "tablero", "tipo_documento", "nombre_archivo", "Documentos_subcontrataley", "flog"]]
        df = agregar_nombre_subcontrataley_indumotora(df, columna_instalacion="instalacion")
        result = df.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


