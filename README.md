🏗️ Arquitectura
┌─────────────────────────────────────────────────────────────────┐
│                    SISTEMA DE CLASIFICACIÓN                      │
├─────────────────────────────────────────────────────────────────┤
│  Videos/Imágenes → YOLO → Detecciones → CSV (Staging)          │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                       SISTEMA ETL                                │
├─────────────────────────────────────────────────────────────────┤
│  CSV → Extract → Transform → Load → Apache Hive                 │
│  (Batch Manager controla flujo por reglas de tiempo/tamaño)     │
└─────────────────────────────────────────────────────────────────┘
🚀 Instalación
Requisitos Previos

Python 3.8+
Apache Hive (con HiveServer2)
CUDA (opcional, para GPU)

Setup
bash# Clonar repositorio
git clone <repository-url>
cd yolo-detection-pipeline

# Crear entorno virtual e instalar dependencias
make setup

# Activar entorno virtual
source venv/bin/activate

# Inicializar Hive (primera vez)
python scripts/run_etl.py --init-hive
📦 Estructura del Proyecto
yolo-detection-pipeline/
├── classification_system/     # Sistema de clasificación YOLO
│   ├── detector.py           # Detección con YOLO
│   ├── video_processor.py    # Procesamiento de videos
│   ├── image_processor.py    # Procesamiento de imágenes
│   └── csv_writer.py         # Escritura a CSV
│
├── etl_system/               # Sistema ETL
│   ├── extractor.py          # Extracción de CSVs
│   ├── transformer.py        # Transformación y limpieza
│   ├── loader.py             # Carga a Hive
│   ├── deduplicator.py       # Deduplicación
│   └── batch_manager.py      # Gestión de lotes
│
├── config/                   # Archivos de configuración
├── scripts/                  # Scripts de ejecución
├── tests/                    # Tests unitarios
└── data/                     # Datos (input, staging, logs)
🎯 Uso
1. Sistema de Clasificación
Procesar un Video
bash# Procesar video específico
python scripts/run_classification.py --video data/input/video.mp4

# Con configuración personalizada
python scripts/run_classification.py --video video.mp4 --config config/custom.yaml
Procesar Imágenes
bash# Procesar directorio de imágenes
python scripts/run_classification.py --images data/input/images/

# Buscar imágenes recursivamente
python scripts/run_classification.py --images data/input/ --recursive
Usar Makefile
bash# Ejecutar sistema de clasificación
make run-classification
2. Sistema ETL
Ejecución Única
bash# Procesar todos los archivos pendientes
python scripts/run_etl.py

# Procesar solo videos
python scripts/run_etl.py --source-type video

# Procesar solo imágenes
python scripts/run_etl.py --source-type image
Modo Daemon (Ejecución Continua)
bash# Ejecutar ETL en modo daemon
python scripts/run_etl.py --daemon

# El sistema verificará automáticamente:
# - Videos: cada 5 minutos (configurable)
# - Imágenes: cuando se complete un lote de 100 (configurable)
Gestión de Estado
bash# Ver estadísticas
python scripts/run_etl.py --stats

# Resetear estado (cuidado!)
python scripts/run_etl.py --reset-state
Usar Makefile
bash# Ejecutar ETL
make run-etl