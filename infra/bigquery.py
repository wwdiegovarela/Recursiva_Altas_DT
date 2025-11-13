from datetime import datetime
from typing import List, Set
import os

import pandas as pd
from google.cloud import bigquery


def cargar_a_bigquery(datos: List[dict], tabla: str = "worldwide-470917.cargas_recursiva.resultado_cargas_altas"):
    try:
        if os.path.exists("service-account.json"):
            client = bigquery.Client.from_service_account_json("service-account.json")
        else:
            client = bigquery.Client(project="worldwide-470917")

        df = pd.DataFrame(datos)

        fecha_original = df['fecha_contrato'].copy()
        df['fecha_contrato_date'] = pd.to_datetime(df['fecha_contrato'], format='%Y-%m-%d', errors='coerce')

        fechas_invalidas = df['fecha_contrato_date'].isna() & df['fecha_contrato'].notna()
        if fechas_invalidas.any():
            indices_invalidos = df[fechas_invalidas].index
            fechas_invalidas_list = fecha_original.iloc[indices_invalidos].tolist()
            raise ValueError(
                "El campo fecha_contrato no tiene el formato correcto. "
                "Formato esperado: YYYY-MM-DD (ej: 2025-01-15). "
                f"Valores inválidos encontrados: {fechas_invalidas_list}"
            )

        if df['fecha_contrato'].isna().any():
            raise ValueError("El campo fecha_contrato no puede estar vacío. Formato requerido: YYYY-MM-DD")

        df['fecha_contrato'] = df['fecha_contrato_date'].dt.date
        df['id'] = df['rut'].astype(str) + '_' + fecha_original.astype(str)
        df = df.drop(columns=['fecha_contrato_date'])
        df['fecha_carga'] = datetime.now()
        df = df[['id', 'rut', 'fecha_contrato', 'estado', 'detalle', 'fecha_carga']]

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True
        )
        job = client.load_table_from_dataframe(df, tabla, job_config=job_config)
        job.result()

        return {"filas_insertadas": len(datos), "job_id": job.job_id, "tabla": tabla}
    except Exception as e:
        raise Exception(f"Error al cargar datos a BigQuery: {type(e).__name__}: {str(e)}")


def obtener_ids_exitosos(tabla: str = "worldwide-470917.cargas_recursiva.resultado_cargas_altas") -> Set[str]:
    try:
        if os.path.exists("service-account.json"):
            client = bigquery.Client.from_service_account_json("service-account.json")
        else:
            client = bigquery.Client(project="worldwide-470917")

        query = f"""
        SELECT CONCAT(CAST(rut AS STRING), '_', CAST(fecha_contrato AS STRING)) AS id
        FROM `{tabla}`
        WHERE estado = 'Exitoso'
        GROUP BY id
        """
        result = client.query(query).result()
        ids = {row.id for row in result}
        return ids
    except Exception as e:
        print(f"Advertencia: no se pudo consultar BigQuery para ids exitosos: {type(e).__name__}: {str(e)}")
        return set()


def cargar_resultados_clientes(datos: List[dict], tabla: str = "worldwide-470917.cargas_recursiva.resultado_cargas_certificadoras_clientes"):
    try:
        if os.path.exists("service-account.json"):
            client = bigquery.Client.from_service_account_json("service-account.json")
        else:
            client = bigquery.Client(project="worldwide-470917")

        df = pd.DataFrame(datos)

        # Validaciones mínimas
        requeridos = ["cliente", "dominio", "endpoint", "fecha_referencia", "estado", "detalle"]
        faltantes = [c for c in requeridos if c not in df.columns]
        if faltantes:
            raise ValueError(f"Faltan columnas requeridas: {faltantes}")

        # Parseo de fecha_referencia
        fecha_original = df["fecha_referencia"].copy()
        df["fecha_referencia_ts"] = pd.to_datetime(df["fecha_referencia"], errors="coerce")
        if df["fecha_referencia_ts"].isna().any():
            indices_invalidos = df[df["fecha_referencia_ts"].isna()].index
            fechas_invalidas = fecha_original.iloc[indices_invalidos].tolist()
            raise ValueError("fecha_referencia inválida. Use ISO 8601 o YYYY-MM-DD. " f"Valores inválidos: {fechas_invalidas}")

        # ID sugerido si no viene: cliente_endpoint_rut_YYYY-MM-DD (rut puede ser vacío)
        if "id" not in df.columns or df["id"].isna().any():
            rut_str = df.get("rut", "").astype(str).fillna("")
            fecha_str = df["fecha_referencia_ts"].dt.strftime("%Y-%m-%d")
            df["id"] = df["cliente"].astype(str) + "_" + df["endpoint"].astype(str) + "_" + rut_str + "_" + fecha_str

        df["fecha_carga"] = datetime.now()

        # Reordenar columnas (no estricto, BigQuery autodetect)
        columnas = [
            "id", "cliente", "dominio", "endpoint", "rut", "instalacion", "cecos",
            "tipo_documento", "nombre_documento", "fecha_referencia_ts", "nombre_archivo",
            "estado", "detalle", "fecha_carga"
        ]
        for c in columnas:
            if c not in df.columns:
                df[c] = None
        df = df[columnas]
        df = df.rename(columns={"fecha_referencia_ts": "fecha_referencia"})

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True
        )
        job = client.load_table_from_dataframe(df, tabla, job_config=job_config)
        job.result()

        return {"filas_insertadas": len(df), "job_id": job.job_id, "tabla": tabla}
    except Exception as e:
        raise Exception(f"Error al cargar resultados clientes a BigQuery: {type(e).__name__}: {str(e)}")