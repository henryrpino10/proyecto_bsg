
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



