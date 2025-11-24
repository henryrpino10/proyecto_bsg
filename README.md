
Python 3.11+
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

# Inicializar Hive
python scripts/run_etl.py --init-hive

Estructura del Proyecto
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

Uso
1. Sistema de Clasificación
Procesar un Video
bash# Procesar video específico

python scripts/run_classification.py --video data/input/video.mp4

Procesar Imágenes
bash# Procesar directorio de imágenes

python scripts/run_classification.py --images data/input/images/

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


# Resetear estado
python scripts/run_etl.py --reset-state


