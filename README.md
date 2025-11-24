# Proyecto BSG - Sistema de Clasificación y ETL

Este proyecto implementa un pipeline completo para la detección de objetos en imágenes y videos utilizando YOLO, seguido de un proceso ETL (Extract, Transform, Load) para almacenar los resultados en Apache Hive.

## Descripción General

El proyecto funciona en dos fases:
- **Fase 1 - Clasificación**: Detecta objetos en archivos multimedia (imágenes/videos) usando YOLO y genera archivos CSV temporales.
- **Fase 2 - ETL**: Procesa los archivos CSV, transforma los datos y los carga en una base de datos Hive para análisis.

## Estructura del Proyecto

```
proyecto_bsg/
├── classification_system/    # Sistema de clasificación YOLO
├── etl_system/              # Sistema ETL
├── scripts/                 # Scripts ejecutables
│   ├── run_classification.py
│   └── run_etl.py
├── config/                  # Archivos de configuración
│   ├── classification_config.yaml
│   └── etl_config.yaml
├── data/                    # Directorio de datos
│   ├── input/              #  COLOCA TUS ARCHIVOS AQUÍ
│   ├── staging/            # Archivos CSV intermedios (generados)
│   └── logs/               # Registros de ejecución
├── requirements.txt        # Dependencias de producción
├── requirements-dev.txt    # Dependencias de desarrollo
└── Makefile               # Comandos de automatización (Linux/WSL)
```

## Prerrequisitos

- Python 3.11+
- Apache Hive (con HiveServer2 activo)

## Configuración (Setup)

### 1. Clonar el repositorio
```bash
git clone https://github.com/henryrpino10/proyecto_bsg.git
cd proyecto_bsg
```

### 2. Crear entorno virtual e instalar dependencias

**En Linux/WSL:**
```bash
make setup
source venv/bin/activate
```

**En Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 3. Configurar Conexión a Hive

> [!IMPORTANT]
> Antes de ejecutar el ETL, debes configurar la conexión a tu servidor Hive en Azure.

Edita el archivo `config/etl_config.yaml` y modifica las siguientes líneas con tus credenciales:

```yaml
# Líneas 20-23 en config/etl_config.yaml
hive:
  host: "TU_IP_DEL_SERVIDOR"      # ← Cambia esto por tu IP
  port: 10000
  database: "detections_db"
  username: "TU_USUARIO"          # ← Cambia esto por tu usuario
```

### 4. Inicializar Base de Datos (Hive)
Una vez configurado Hive, inicializa las tablas:
```bash
python scripts/run_etl.py --init-hive
```

> [!NOTE]
> El proyecto crea **automáticamente** la base de datos (`detections_db`) y la tabla (`detections`) en Hive. Solo necesitas configurar la conexión (paso 3) y ejecutar el comando anterior. No es necesario crear nada manualmente en Hive.

## Preparación de Datos

**Antes de ejecutar la clasificación, coloca tus archivos en el directorio `data/input/`:**

```bash
# Para imágenes: cópialas directamente
data/input/

# Para videos: coloca los archivos MP4
data/input/
```

> [!NOTE]
> El sistema crea automáticamente los directorios necesarios (`data/staging/`, `data/logs/`) si no existen.

## Uso

### Fase 1: Sistema de Clasificación

El sistema procesa archivos de entrada y genera detecciones en formato CSV en `data/staging/`.

**Procesar Imágenes:**
```bash
python scripts/run_classification.py --images data/input/
```

**Procesar Videos:**
Ejemplo para procesar los videos cargados (`vid1.mp4`, `vid2.mp4`, `vid3.mp4`):
```bash
python scripts/run_classification.py --video data/input/vid1.mp4
python scripts/run_classification.py --video data/input/vid2.mp4
python scripts/run_classification.py --video data/input/vid3.mp4
```

**Resultados de Fase 1:**
- Los archivos CSV se generan en: `data/staging/`
- Nombres de archivo: `video_detections_vid1_YYYYMMDD_HHMMSS.csv` o `image_detections_YYYYMMDD_HHMMSS.csv`
- Logs de ejecución en: `data/logs/classification_*.log`

### Fase 2: Sistema ETL

El sistema ETL toma los datos generados en la fase de clasificación y los carga en Hive.

**Ejecución estándar (procesar todo lo pendiente):**
```bash
python scripts/run_etl.py
```

**Opciones adicionales:**

- Resetear estado (reprocesar todo): `python scripts/run_etl.py --reset-state`
- Ver estadísticas: `python scripts/run_etl.py --stats`

**Resultados de Fase 2:**
- Datos cargados en Hive: tabla `detections_db.detections`
- Estado del ETL guardado en: `data/etl_state.json`
- Logs de ejecución en: `data/logs/etl_*.log`

## Ejemplo Rápido (Flujo Completo)

```bash
# 1. Coloca tu video en el directorio de entrada
cp /ruta/a/tu/video.mp4 data/input/

# 2. Ejecuta la clasificación
python scripts/run_classification.py --video data/input/video.mp4

# 3. Verifica que se generó el CSV en staging
ls data/staging/

# 4. Ejecuta el ETL para cargar a Hive
python scripts/run_etl.py

# 5. Verifica los resultados
python scripts/run_etl.py --stats
```

## Comandos Útiles

Ver todos los comandos disponibles (Linux/WSL):
```bash
make help
```

Limpiar archivos temporales:
```bash
make clean
```

Ejecutar tests:
```bash
make test
```
