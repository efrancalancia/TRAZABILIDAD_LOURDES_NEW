# 🍇 TRAZABILIDAD LOURDES

Sistema de trazabilidad de lotes para bodega - API FastAPI + Frontend Streamlit

## 📋 Descripción

Este proyecto proporciona una solución completa para la trazabilidad de lotes en una bodega, permitiendo rastrear el origen y destino de cada lote de vino desde la materia prima hasta el producto final.

### Componentes

| Componente | Tecnología | Descripción |
|------------|------------|-------------|
| **Backend** | FastAPI | API REST para consultas de trazabilidad |
| **Frontend** | Streamlit | Interfaz web interactiva |
| **Base de Datos** | Oracle | Conexión a base de datos Oracle |

---

## 🚀 Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/efrancalancia/TRAZABILIDAD_LOURDES_NEW.git
cd TRAZABILIDAD_LOURDES_NEW
```

### 2. Crear y activar el entorno virtual

**Windows (PowerShell):**
```powershell
# Crear entorno virtual
python -m venv .venv

# Activar entorno virtual
.venv\Scripts\Activate.ps1
```

**Windows (CMD):**
```cmd
# Crear entorno virtual
python -m venv .venv

# Activar entorno virtual
.venv\Scripts\activate.bat
```

**Linux/macOS:**
```bash
# Crear entorno virtual
python3 -m venv .venv

# Activar entorno virtual
source .venv/bin/activate
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

---

## ⚙️ Configuración del archivo `.env`

Crear un archivo `.env` en la raíz del proyecto con las siguientes variables:

```env
# --- Oracle ---
ORACLE_TNS_ALIAS=NOMBRE_TNS           # Alias del TNS configurado en tnsnames.ora
ORACLE_TNS_ADMIN=C:\oracle\instantclient_21_8\network\admin  # Ruta al directorio con tnsnames.ora
DB_CREDENTIALS_PATH=C:\ruta\a\credenciales.pwd  # Ruta al archivo con credenciales (opcional)
DB_SCHEMA=NOMBRE_SCHEMA               # Esquema de base de datos

# --- Credenciales directas (alternativa a DB_CREDENTIALS_PATH) ---
# DB_USERNAME=usuario
# DB_PASSWORD=contraseña

# --- Límites / constantes ---
ORACLE_IN_CLAUSE_LIMIT=999            # Límite de elementos en cláusula IN de Oracle
MAX_LEN_D_DEPOSITO=20                 # Longitud máxima del nombre de depósito

# --- API ---
API_HOST=0.0.0.0                      # Host de la API (0.0.0.0 para todas las interfaces)
API_PORT=8000                         # Puerto de la API
LOG_LEVEL=INFO                        # Nivel de logging (DEBUG, INFO, WARNING, ERROR)

# --- Modo de Trazabilidad ---
TRACE_MODE=fake                       # fake = datos de prueba, real = conexión a Oracle

# --- Salidas ---
CSV_OUT_DIR=./outputs                 # Directorio para archivos CSV generados
LOGS_OUT_DIR=./outputs/logs           # Directorio para logs
BACKEND_BASE_URL=http://localhost:8000  # URL base del backend para el frontend

# --- Módulo de composición ---
COMPOSICION_MODULE_PATH=./composicion_enologica.py  # Ruta al módulo de composición
```

### Formato del archivo de credenciales

El archivo especificado en `DB_CREDENTIALS_PATH` puede tener uno de estos formatos:

**Opción 1 - Dos líneas (recomendado):**
```
usuario
contraseña
```

**Opción 2 - JSON:**
```json
{"username": "usuario", "password": "contraseña"}
```

**Opción 3 - KEY=VALUE:**
```
DB_USERNAME=usuario
DB_PASSWORD=contraseña
```

---

## 🖥️ Ejecución

### Backend (FastAPI)

```bash
# Desde la raíz del proyecto, con el entorno virtual activado
cd backend
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

La API estará disponible en:
- **Documentación Swagger**: http://localhost:8000/docs
- **Documentación ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

### Frontend (Streamlit)

```bash
# Desde la raíz del proyecto, con el entorno virtual activado
cd frontend/streamlit_app
streamlit run Home.py
```

La aplicación Streamlit estará disponible en: http://localhost:8501

---

## 📁 Estructura del Proyecto

```
TRAZABILIDAD_LOURDES_NEW/
├── .env                         # Configuración (NO incluido en Git)
├── .gitignore
├── requirements.txt             # Dependencias Python
├── composicion_enologica.py     # Módulo de composición enológica
│
├── backend/                     # API FastAPI
│   ├── __init__.py
│   └── app/
│       ├── main.py              # Punto de entrada
│       ├── api/v1/              # Endpoints
│       │   ├── health.py        # /api/health
│       │   ├── composicion.py   # /api/composicion
│       │   └── trazabilidad.py  # /api/trazabilidad
│       ├── core/
│       │   └── config.py        # Configuración desde .env
│       ├── models/
│       │   └── schemas.py       # Modelos Pydantic
│       └── services/
│           ├── db.py            # Conexión Oracle
│           ├── composicion/
│           └── trazabilidad/
│
├── frontend/                    # Frontend Streamlit
│   └── streamlit_app/
│       ├── Home.py
│       └── pages/
│           ├── 1_Ejecutar_Proceso.py
│           └── 2_Reporte_Trazabilidad.py
│
└── outputs/                     # Archivos generados
```

---

## 🔗 Endpoints principales

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/api/health` | Estado de salud de la API |
| GET | `/api/health/deep` | Estado detallado incluyendo conexión a Oracle |
| GET | `/api/trazabilidad/{c_lote}` | Consulta trazabilidad de un lote |
| POST | `/api/composicion/run` | Ejecutar proceso de composición |

---

## 🧪 Tests

```bash
# Ejecutar tests
pytest

# Con cobertura
pytest --cov=backend
```

---

## 📝 Notas importantes

1. **Oracle Instant Client**: Debe estar instalado y configurado para la conexión a Oracle
2. **TNS**: El archivo `tnsnames.ora` debe contener el alias especificado en `ORACLE_TNS_ALIAS`
3. **Modo fake**: Por defecto `TRACE_MODE=fake` usa datos de prueba sin necesidad de Oracle
4. **Seguridad**: El archivo `.env` está en `.gitignore` y NO se sube al repositorio

---

## 📄 Licencia

Proyecto interno - Bodega Lourdes
