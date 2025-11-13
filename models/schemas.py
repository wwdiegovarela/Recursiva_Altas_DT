from pydantic import BaseModel
from typing import List


class ResultadoCarga(BaseModel):
    fecha_contrato: str
    rut: str
    estado: str
    detalle: str


class ResultadoCargasRequest(BaseModel):
    datos: List[ResultadoCarga]


# Esquemas específicos para Bajas
class ResultadoCargaBaja(BaseModel):
    fecha_finiquito: str
    rut: str
    estado: str
    detalle: str


class ResultadoCargasBajasRequest(BaseModel):
    datos: List[ResultadoCargaBaja]


# Resultados unificados para clientes (no-DT)
class ResultadoCliente(BaseModel):
    cliente: str
    dominio: str
    endpoint: str
    rut: str | None = None
    instalacion: str | None = None
    cecos: str | None = None
    tipo_documento: str | None = None
    nombre_documento: str | None = None
    fecha_referencia: str  # ISO 8601 o YYYY-MM-DD
    nombre_archivo: str | None = None
    estado: str
    detalle: str


class ResultadoClientesRequest(BaseModel):
    datos: List[ResultadoCliente]