from fastapi import APIRouter
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
import traceback

from config import (
    TOKEN_DOC_FIRMA_TELEFONICA,
    TOKEN_DOC_CARPETA_TELEFONICA,
    TOKEN_ASISTENCIA_TELEFONICA,
    TOKEN_TRANSFERENCIAS_TELEFONICA,
)
from services.utils import (
    intervalo_fechas,
    consulta_cr,
    get_mantenedor,
    traducir_mes_en_espanol,
    agregar_nombre_subcontrataley_telefonica,
)


router = APIRouter(prefix="/subcontrataley/telefonica")


@router.get("/health")  # optional local health for this router
def health_local():
    return {"status": "ok", "service": "telefonica"}


@router.get("/kpr")
def get_kpr():
    try:
        fecha_desde, fecha_hasta = intervalo_fechas()
        data_firma = consulta_cr(TOKEN_DOC_FIRMA_TELEFONICA)
        data_firma["flog"] = pd.to_datetime(data_firma["flog"], format="%Y-%m-%d %H:%M:%S")
        data_firma = data_firma.loc[(data_firma.flog >= fecha_desde) & (data_firma.flog <= fecha_hasta)]
        data_firma = data_firma.loc[data_firma.firma_del_colaborador == "Firmado Colaborador"][["rut", "nombre_del_documento", "tipo_del_documento", "flog"]]
        data_firma["tipo_del_documento"].loc[
            (data_firma.tipo_del_documento == "AnexoPersonalizado")
            & (data_firma.nombre_del_documento == "KIT PREVENCION DE RIESGOS")
        ] = "KIT PREVENCION DE RIESGOS"
        data_firma = data_firma.loc[data_firma["tipo_del_documento"] == "KIT PREVENCION DE RIESGOS"]
        data_firma = data_firma.merge(
            get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta", "documento_cr_carpeta2"]],
            left_on="tipo_del_documento",
            right_on="documento_cr_carpeta2",
            how="left",
        )
        data_firma = data_firma[["rut", "modulo", "tablero", "documento_cr_carpeta", "tipo_del_documento", "nombre_del_documento", "Documentos_subcontrataley", "flog"]]
        result = data_firma.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": fecha_desde.isoformat(), "hasta": fecha_hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/contrato")
def get_contrato():
    try:
        fecha_desde, fecha_hasta = intervalo_fechas()
        data_firma = consulta_cr(TOKEN_DOC_FIRMA_TELEFONICA)
        data_firma["flog"] = pd.to_datetime(data_firma["flog"], format="%Y-%m-%d %H:%M:%S")
        data_firma = data_firma.loc[(data_firma.flog >= fecha_desde) & (data_firma.flog <= fecha_hasta)]
        data_firma = data_firma.loc[data_firma.firma_del_colaborador == "Firmado Colaborador"][["rut", "nombre_del_documento", "tipo_del_documento", "flog"]]
        data_firma["tipo_del_documento"].loc[
            (data_firma.tipo_del_documento == "AnexoPersonalizado")
            & (data_firma.nombre_del_documento == "KIT PREVENCION DE RIESGOS")
        ] = "KIT PREVENCION DE RIESGOS"
        data_firma = data_firma.loc[data_firma["tipo_del_documento"] == "Contrato"]
        data_firma = data_firma.merge(
            get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta", "documento_cr_carpeta2"]],
            left_on="tipo_del_documento",
            right_on="documento_cr_carpeta2",
            how="left",
        )
        data_firma = data_firma[["rut", "modulo", "tablero", "documento_cr_carpeta", "tipo_del_documento", "nombre_del_documento", "Documentos_subcontrataley", "flog"]]
        result = data_firma.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": fecha_desde.isoformat(), "hasta": fecha_hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


def _carpeta_filtrada(tipo, multiple: bool = False):
    fecha_desde, fecha_hasta = intervalo_fechas()
    data_norm = consulta_cr(TOKEN_DOC_CARPETA_TELEFONICA)
    data_norm["flog"] = pd.to_datetime(data_norm["flog"], format="%Y-%m-%d %H:%M:%S")
    data_norm = data_norm.loc[(data_norm.flog >= fecha_desde) & (data_norm.flog <= fecha_hasta)]
    data_norm = data_norm[["rut", "tipo_documento", "nombre_documento", "flog"]]
    if multiple:
        data_norm = data_norm.loc[data_norm["tipo_documento"].isin(tipo)]
    else:
        data_norm = data_norm.loc[data_norm["tipo_documento"] == tipo]
    data_norm = data_norm.merge(
        get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta", "documento_cr_carpeta2"]],
        left_on="tipo_documento",
        right_on="documento_cr_carpeta2",
        how="left",
    )
    data_norm = data_norm[["rut", "modulo", "tablero", "documento_cr_carpeta", "tipo_documento", "nombre_documento", "Documentos_subcontrataley", "flog"]]
    result = data_norm.replace({np.nan: None}).to_dict(orient="records")
    return fecha_desde, fecha_hasta, result


@router.get("/finiquito")
def get_finiquito():
    try:
        desde, hasta, result = _carpeta_filtrada("Finiquito")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/epp")
def get_epp():
    try:
        desde, hasta, result = _carpeta_filtrada("Entrega de EPP")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/os10")
def get_os10():
    try:
        desde, hasta, result = _carpeta_filtrada("Certificado Curso")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/antecedentes")
def get_antecedentes():
    try:
        desde, hasta, result = _carpeta_filtrada("Certificado Antecedentes")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/cedula")
def get_cedula():
    try:
        desde, hasta, result = _carpeta_filtrada("Cedula Identidad")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/cdrv")
def get_cdrv():
    try:
        desde, hasta, result = _carpeta_filtrada(["CARTAS DESPIDO", "RENUNCIA VOLUNTARIA"], multiple=True)
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/cuepp")
def get_cuepp():
    try:
        desde, hasta, result = _carpeta_filtrada("Capacitacion Uso EPP")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/anexotraslado")
def get_anexotraslado():
    try:
        desde, hasta, result = _carpeta_filtrada("AnexoTraslado")
        return {"ok": True, "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/asistencia")
def get_asistencia():
    try:
        data_asistencia = consulta_cr(TOKEN_ASISTENCIA_TELEFONICA)
        data_asistencia = data_asistencia.loc[data_asistencia["faceid_enrolado"] == "SI"]
        fecha_desde, fecha_hasta = intervalo_fechas()
        data_asistencia = data_asistencia[["rut", "cliente", "instalacion", "cecos"]]
        data_asistencia["tipo_documento"] = "Asistencia"
        data_asistencia = data_asistencia.merge(
            get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta"]],
            left_on="tipo_documento",
            right_on="documento_cr_carpeta",
            how="left",
        )
        data_asistencia["Desde"] = fecha_desde.strftime("%d-%m-%Y")
        data_asistencia["Hasta"] = fecha_hasta.strftime("%d-%m-%Y")
        data_asistencia = agregar_nombre_subcontrataley_telefonica(data_asistencia, columna_instalacion="instalacion")
        result = data_asistencia.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": fecha_desde.strftime("%d-%m-%Y"), "hasta": fecha_hasta.strftime("%d-%m-%Y")}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/liquidaciones")
def get_liquidaciones():
    try:
        data_liquidaciones = consulta_cr(TOKEN_ASISTENCIA_TELEFONICA)
        data_liquidaciones = data_liquidaciones[["cliente", "instalacion", "cecos"]].drop_duplicates()
        data_liquidaciones["tipo_documento"] = "Liquidaciones"
        data_liquidaciones = data_liquidaciones.merge(
            get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "tablero2"]],
            left_on="tipo_documento",
            right_on="tablero",
            how="left",
        )
        data_liquidaciones = data_liquidaciones[["instalacion", "cecos", "modulo", "tablero", "Documentos_subcontrataley"]]
        _, fecha_hasta = intervalo_fechas()
        data_liquidaciones["periodo"] = fecha_hasta
        data_liquidaciones["periodo"] = data_liquidaciones["periodo"].dt.strftime("%B %Y")
        data_liquidaciones["periodo"] = data_liquidaciones["periodo"].apply(traducir_mes_en_espanol)
        data_liquidaciones = agregar_nombre_subcontrataley_telefonica(data_liquidaciones, columna_instalacion="instalacion")
        result = data_liquidaciones.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": fecha_hasta.strftime("%B %Y"), "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})


@router.get("/transferencias")
def get_transferencias():
    try:
        data_transferencias = consulta_cr(TOKEN_TRANSFERENCIAS_TELEFONICA)
        data_transferencias["tipo_documento"] = "TRANSFERENCIA"
        data_transferencias = data_transferencias.merge(
            get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "tablero2", "tablero3", "documento_cr_carpeta"]],
            left_on="tipo_documento",
            right_on="documento_cr_carpeta",
            how="left",
        )
        data_transferencias = data_transferencias[["instalacion", "codcecoscr", "modulo", "tablero", "tipo_documento", "nombre_archivo", "Documentos_subcontrataley", "flog"]]
        data_transferencias = agregar_nombre_subcontrataley_telefonica(data_transferencias, columna_instalacion="instalacion")
        result_json = data_transferencias.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "total_registros": len(result_json), "data": result_json}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()})

from fastapi import APIRouter
from fastapi.responses import JSONResponse
import numpy as np
import pandas as pd

from config import (
    TOKEN_DOC_FIRMA_TELEFONICA,
    TOKEN_DOC_CARPETA_TELEFONICA,
    TOKEN_ASISTENCIA_TELEFONICA,
    TOKEN_TRANSFERENCIAS_TELEFONICA,
)
from services.utils import (
    intervalo_fechas,
    consulta_cr,
    get_mantenedor,
    traducir_mes_en_espanol,
    agregar_nombre_subcontrataley_telefonica,
)

router = APIRouter(prefix="/subcontrataley/telefonica")


@router.get("/kpr")
def get_kpr():
    try:
        fecha_desde, fecha_hasta = intervalo_fechas()
        data_firma = consulta_cr(TOKEN_DOC_FIRMA_TELEFONICA)
        data_firma['flog'] = pd.to_datetime(data_firma['flog'], format='%Y-%m-%d %H:%M:%S')
        data_firma = data_firma.loc[(data_firma.flog >= fecha_desde) & (data_firma.flog <= fecha_hasta)]
        data_firma = data_firma.loc[data_firma.firma_del_colaborador == "Firmado Colaborador"][['rut', 'nombre_del_documento', 'tipo_del_documento', 'flog']]
        data_firma["tipo_del_documento"].loc[(data_firma.tipo_del_documento == "AnexoPersonalizado") & (data_firma.nombre_del_documento == "KIT PREVENCION DE RIESGOS")] = "KIT PREVENCION DE RIESGOS"
        data_firma = data_firma.loc[data_firma["tipo_del_documento"] == "KIT PREVENCION DE RIESGOS"]
        data_firma = data_firma.merge(get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta", "documento_cr_carpeta2"]], left_on="tipo_del_documento", right_on="documento_cr_carpeta2", how="left")
        data_firma = data_firma[["rut", "modulo", "tablero", "documento_cr_carpeta", "tipo_del_documento", "nombre_del_documento", "Documentos_subcontrataley", "flog"]]
        result = data_firma.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": fecha_desde.isoformat(), "hasta": fecha_hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}"})


@router.get("/contrato")
def get_contrato():
    try:
        fecha_desde, fecha_hasta = intervalo_fechas()
        data_firma = consulta_cr(TOKEN_DOC_FIRMA_TELEFONICA)
        data_firma['flog'] = pd.to_datetime(data_firma['flog'], format='%Y-%m-%d %H:%M:%S')
        data_firma = data_firma.loc[(data_firma.flog >= fecha_desde) & (data_firma.flog <= fecha_hasta)]
        data_firma = data_firma.loc[data_firma.firma_del_colaborador == "Firmado Colaborador"][['rut', 'nombre_del_documento', 'tipo_del_documento', 'flog']]
        data_firma["tipo_del_documento"].loc[(data_firma.tipo_del_documento == "AnexoPersonalizado") & (data_firma.nombre_del_documento == "KIT PREVENCION DE RIESGOS")] = "KIT PREVENCION DE RIESGOS"
        data_firma = data_firma.loc[data_firma["tipo_del_documento"] == "Contrato"]
        data_firma = data_firma.merge(get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta", "documento_cr_carpeta2"]], left_on="tipo_del_documento", right_on="documento_cr_carpeta2", how="left")
        data_firma = data_firma[["rut", "modulo", "tablero", "documento_cr_carpeta", "tipo_del_documento", "nombre_del_documento", "Documentos_subcontrataley", "flog"]]
        result = data_firma.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": fecha_desde.isoformat(), "hasta": fecha_hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}"})


def _carpeta_common(tipo_documento: str = None, tipos_documento_in: list = None):
    fecha_desde, fecha_hasta = intervalo_fechas()
    data_norm = consulta_cr(TOKEN_DOC_CARPETA_TELEFONICA)
    data_norm['flog'] = pd.to_datetime(data_norm['flog'], format='%Y-%m-%d %H:%M:%S')
    data_norm = data_norm.loc[(data_norm.flog >= fecha_desde) & (data_norm.flog <= fecha_hasta)]
    data_norm = data_norm[["rut", "tipo_documento", "nombre_documento", "flog"]]
    if tipo_documento:
        data_norm = data_norm.loc[data_norm["tipo_documento"] == tipo_documento]
    if tipos_documento_in:
        data_norm = data_norm.loc[data_norm["tipo_documento"].isin(tipos_documento_in)]
    data_norm = data_norm.merge(get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta", "documento_cr_carpeta2"]], left_on="tipo_documento", right_on="documento_cr_carpeta2", how="left")
    data_norm = data_norm[["rut", "modulo", "tablero", "documento_cr_carpeta", "tipo_documento", "nombre_documento", "Documentos_subcontrataley", "flog"]]
    return fecha_desde, fecha_hasta, data_norm


@router.get("/finiquito")
def get_finiquito():
    try:
        fecha_desde, fecha_hasta, data = _carpeta_common(tipo_documento="Finiquito")
        result = data.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": fecha_desde.isoformat(), "hasta": fecha_hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}"})


@router.get("/epp")
def get_epp():
    try:
        fecha_desde, fecha_hasta, data = _carpeta_common(tipo_documento="Entrega de EPP")
        result = data.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": fecha_desde.isoformat(), "hasta": fecha_hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}"})


@router.get("/os10")
def get_os10():
    try:
        fecha_desde, fecha_hasta, data = _carpeta_common(tipo_documento="Certificado Curso")
        result = data.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": fecha_desde.isoformat(), "hasta": fecha_hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}"})


@router.get("/antecedentes")
def get_antecedentes():
    try:
        fecha_desde, fecha_hasta, data = _carpeta_common(tipo_documento="Certificado Antecedentes")
        result = data.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": fecha_desde.isoformat(), "hasta": fecha_hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}"})


@router.get("/cedula")
def get_cedula():
    try:
        fecha_desde, fecha_hasta, data = _carpeta_common(tipo_documento="Cedula Identidad")
        result = data.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": fecha_desde.isoformat(), "hasta": fecha_hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}"})


@router.get("/cdrv")
def get_cdrv():
    try:
        fecha_desde, fecha_hasta, data = _carpeta_common(tipos_documento_in=["CARTAS DESPIDO", "RENUNCIA VOLUNTARIA"])
        result = data.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": fecha_desde.isoformat(), "hasta": fecha_hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}"})


@router.get("/cuepp")
def get_cuepp():
    try:
        fecha_desde, fecha_hasta, data = _carpeta_common(tipo_documento="Capacitacion Uso EPP")
        result = data.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": fecha_desde.isoformat(), "hasta": fecha_hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}"})


@router.get("/anexotraslado")
def get_anexotraslado():
    try:
        fecha_desde, fecha_hasta, data = _carpeta_common(tipo_documento="AnexoTraslado")
        result = data.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": fecha_desde.isoformat(), "hasta": fecha_hasta.isoformat()}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}"})


@router.get("/asistencia")
def get_asistencia():
    try:
        data_asistencia = consulta_cr(TOKEN_ASISTENCIA_TELEFONICA)
        data_asistencia = data_asistencia.loc[data_asistencia['faceid_enrolado'] == "SI"]
        fecha_desde, fecha_hasta = intervalo_fechas()
        data_asistencia = data_asistencia[["rut", "cliente", "instalacion", "cecos"]]
        data_asistencia['tipo_documento'] = 'Asistencia'
        data_asistencia = data_asistencia.merge(get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "documento_cr_carpeta"]], left_on="tipo_documento", right_on="documento_cr_carpeta", how="left")
        data_asistencia['Desde'] = fecha_desde.strftime('%d-%m-%Y')
        data_asistencia['Hasta'] = fecha_hasta.strftime('%d-%m-%Y')
        data_asistencia = agregar_nombre_subcontrataley_telefonica(data_asistencia, columna_instalacion="instalacion")
        result = data_asistencia.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": {"desde": fecha_desde.strftime('%d-%m-%Y'), "hasta": fecha_hasta.strftime('%d-%m-%Y')}, "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}"})


@router.get("/liquidaciones")
def get_liquidaciones():
    try:
        data_liquidaciones = consulta_cr(TOKEN_ASISTENCIA_TELEFONICA)
        data_liquidaciones = data_liquidaciones[["cliente", "instalacion", "cecos"]].drop_duplicates()
        data_liquidaciones['tipo_documento'] = 'Liquidaciones'
        data_liquidaciones = data_liquidaciones.merge(get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "tablero2"]], left_on="tipo_documento", right_on="tablero", how="left")
        data_liquidaciones = data_liquidaciones[["instalacion", "cecos", "modulo", "tablero", "Documentos_subcontrataley"]]
        _, fecha_hasta = intervalo_fechas()
        data_liquidaciones['periodo'] = fecha_hasta
        data_liquidaciones['periodo'] = data_liquidaciones['periodo'].dt.strftime('%B %Y')
        data_liquidaciones['periodo'] = data_liquidaciones['periodo'].apply(traducir_mes_en_espanol)
        data_liquidaciones = agregar_nombre_subcontrataley_telefonica(data_liquidaciones, columna_instalacion="instalacion")
        result = data_liquidaciones.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "periodo": fecha_hasta.strftime('%B %Y'), "total_registros": len(result), "data": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}"})


@router.get("/transferencias")
def get_transferencias():
    try:
        data_transferencias = consulta_cr(TOKEN_TRANSFERENCIAS_TELEFONICA)
        data_transferencias['tipo_documento'] = 'TRANSFERENCIA'
        data_transferencias = data_transferencias.merge(get_mantenedor()[["Documentos_subcontrataley", "modulo", "tablero", "tablero2", "tablero3", "documento_cr_carpeta"]], left_on="tipo_documento", right_on="documento_cr_carpeta", how="left")
        data_transferencias = data_transferencias[["instalacion", "codcecoscr", "modulo", "tablero", "tipo_documento", "nombre_archivo", "Documentos_subcontrataley", "flog"]]
        data_transferencias = agregar_nombre_subcontrataley_telefonica(data_transferencias, columna_instalacion="instalacion")
        result_json = data_transferencias.replace({np.nan: None}).to_dict(orient="records")
        return {"ok": True, "total_registros": len(result_json), "data": result_json}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {str(e)}"})

