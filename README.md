# Recursiva API - WFSA ControlRoll

API REST para la sincronización de datos de empleados desde ControlRoll para Worldwide Facility Security S.A.

## 🌐 URL de Producción

```
https://recursiva-data-altas-dt-596669043554.us-east1.run.app
```

## 📋 Descripción

Este servicio de Cloud Run proporciona endpoints REST para consultar y procesar datos de:
- SubcontrataLey Walmart (firmas, documentos, asistencia, liquidaciones, transferencias)
- Dirección del Trabajo (altas de empleados)

## 📁 Archivos del proyecto

- `main.py` - Aplicación FastAPI principal
- `requirements.txt` - Dependencias de Python
- `Dockerfile` - Configuración de Docker para Cloud Run
- `README.md` - Este archivo

## 🔧 Variables de entorno requeridas

Configura las siguientes variables de entorno en Cloud Run:

### API Externa
- `API_LOCAL_URL` - URL de la API de ControlRoll

### Tokens de autenticación
- `TOKEN_ALTAS` - Token para datos de altas (primer dataset)
- `TOKEN_ALTAS2` - Token para datos de altas (segundo dataset)
- `TOKEN_DOC_FIRMA_WALMART` - Token para documentos firmados de Walmart
- `TOKEN_DOC_CARPETA_WALMART` - Token para carpeta de documentos de Walmart
- `TOKEN_ASISTENCIA_WALMART` - Token para asistencia y liquidaciones de Walmart
- `TOKEN_TRANSFERENCIAS_WALMART` - Token para transferencias de Walmart

## 🚀 Endpoints disponibles

### Health Check
**GET** `/health`

Verifica que el servicio esté funcionando.

**Respuesta:**
```json
{
  "status": "ok"
}
```

---

### SubcontrataLey Walmart

#### 1. Firmas
**GET** `/subcontrataley/walmart/firmas`

Obtiene datos de documentos firmados del mes anterior.

**Respuesta:**
```json
{
  "ok": true,
  "periodo": {
    "desde": "2025-09-01T00:00:00",
    "hasta": "2025-09-30T23:59:59"
  },
  "total_registros": 150,
  "data": [...]
}
```

#### 2. Carpeta de Documentos
**GET** `/subcontrataley/walmart/carpeta`

Obtiene datos normalizados de documentos de carpeta del mes anterior.

#### 3. Asistencia
**GET** `/subcontrataley/walmart/asistencia`

Obtiene datos de asistencia de empleados con FaceID enrolado.

#### 4. Liquidaciones
**GET** `/subcontrataley/walmart/liquidaciones`

Obtiene datos de liquidaciones por instalación del mes anterior.

#### 5. Transferencias
**GET** `/subcontrataley/walmart/transferencias`

Obtiene datos de transferencias bancarias.

---

### Dirección del Trabajo

#### Altas
**GET** `/dt/altas`

Procesa y retorna datos de altas de empleados para la Dirección del Trabajo.

**Respuesta:**
```json
{
  "ok": true,
  "sample": "...",
  "data": [
    {
      "NOMBRE_EMPRESA": "WORLDWIDE FACILITY SECURITY S.A.",
      "RUT_EMPRESA": "76195703-1",
      "RUT_TRABAJADOR": "12345678-9",
      "NOMBRES": "Juan",
      "APELLIDOS": "Pérez González",
      ...
    }
  ]
}
```

## 📚 Documentación automática

Una vez desplegado, puedes acceder a la documentación interactiva:

- **Swagger UI**: https://recursiva-data-altas-dt-596669043554.us-east1.run.app/docs
- **ReDoc**: https://recursiva-data-altas-dt-596669043554.us-east1.run.app/redoc

## 🐳 Despliegue a Cloud Run

### Opción 1: Usando gcloud CLI

```bash
# Autenticarse
gcloud auth login

# Construir y desplegar
gcloud builds submit --tag gcr.io/TU_PROJECT_ID/recursiva-api

gcloud run deploy recursiva-data-altas-dt \
  --image gcr.io/TU_PROJECT_ID/recursiva-api \
  --platform managed \
  --region us-east1 \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600 \
  --set-env-vars API_LOCAL_URL="https://cl.controlroll.com/ww01/ServiceUrl.aspx",\
TOKEN_ALTAS="tu-token",\
TOKEN_ALTAS2="tu-token",\
TOKEN_DOC_FIRMA_WALMART="tu-token",\
TOKEN_DOC_CARPETA_WALMART="tu-token",\
TOKEN_ASISTENCIA_WALMART="tu-token",\
TOKEN_TRANSFERENCIAS_WALMART="tu-token"
```

### Opción 2: Usando Google Cloud Console

1. Ve a [Google Cloud Console](https://console.cloud.google.com/)
2. Navega a **Cloud Run**
3. Haz clic en **"Crear servicio"**
4. Configura:
   - **Nombre**: `recursiva-data-altas-dt`
   - **Región**: `us-east1`
   - **Autenticación**: Permitir tráfico no autenticado
5. En **"Código fuente"**:
   - Conecta tu repositorio de GitHub
   - Selecciona la rama `main`
6. En **"Variables de entorno"**:
   - Agrega todas las variables listadas arriba
7. En **"Capacidad"**:
   - Memoria: 2 GiB
   - CPU: 2
   - Timeout: 3600 segundos
8. Haz clic en **"Crear"**

## 💻 Desarrollo local

### Requisitos

- Python 3.11+
- pip

### Instalación

```bash
# Clonar repositorio
git clone https://github.com/wwdiegovarela/Recursiva_Altas_DT.git
cd Recursiva_Altas_DT

# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno
export API_LOCAL_URL="https://cl.controlroll.com/ww01/ServiceUrl.aspx"
export TOKEN_ALTAS="tu-token"
export TOKEN_ALTAS2="tu-token"
export TOKEN_DOC_FIRMA_WALMART="tu-token"
export TOKEN_DOC_CARPETA_WALMART="tu-token"
export TOKEN_ASISTENCIA_WALMART="tu-token"
export TOKEN_TRANSFERENCIAS_WALMART="tu-token"

# Iniciar servidor
uvicorn main:app --reload --host 0.0.0.0 --port 8080
```

En Windows PowerShell:
```powershell
$env:API_LOCAL_URL="https://cl.controlroll.com/ww01/ServiceUrl.aspx"
$env:TOKEN_ALTAS="tu-token"
# ... etc

uvicorn main:app --reload --host 0.0.0.0 --port 8080
```

Accede a:
- API: http://localhost:8080
- Docs: http://localhost:8080/docs

## 🧪 Pruebas con Postman

Se incluye una colección de Postman para facilitar las pruebas:

1. Importa `Recursiva_API.postman_collection.json` en Postman
2. (Opcional) Importa `Recursiva_API.postman_environment.json`
3. Los endpoints ya están configurados con la URL de producción

## 📊 Monitoreo

Para monitorear el servicio en producción:

1. Accede a [Google Cloud Console](https://console.cloud.google.com/)
2. Navega a **Cloud Run**
3. Selecciona el servicio `recursiva-data-altas-dt`
4. Revisa:
   - **Métricas** - Latencia, requests, errores
   - **Logs** - Registros detallados de ejecución
   - **Revisiones** - Historial de deployments

## 🔍 Funciones de utilidad

El código incluye varias funciones de utilidad:

- `traducir_mes_en_espanol()` - Traduce nombres de meses
- `agregar_nombre_subcontrataley_walmart()` - Mapea instalaciones Walmart
- `intervalo_fechas()` - Calcula fechas del mes anterior
- `consulta_cr()` - Cliente HTTP para ControlRoll
- `get_mantenedor()` - Configuración de documentos SubcontrataLey

## ⚠️ Notas importantes

- Los datos se filtran automáticamente por el **mes anterior**
- Los endpoints pueden tardar varios minutos en responder (timeout: 1 hora)
- Todos los tokens se configuran en Cloud Run, no en el código
- Las respuestas incluyen manejo de errores con códigos HTTP apropiados

## 📞 Soporte

Para problemas o consultas:
- Email: diego.varela@wfsa.cl
- Revisar logs en Cloud Run para diagnósticos detallados

## 📄 Licencia

© 2025 Worldwide Facility Security S.A.
