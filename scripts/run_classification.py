"""
Script principal para ejecutar el sistema de clasificación.
"""

import sys
from pathlib import Path
import yaml
import argparse
from loguru import logger

# Añadir path del proyecto
sys.path.append(str(Path(__file__).parent.parent))

from classification_system.detector import ObjectDetector
from classification_system.video_processor import VideoProcessor
from classification_system.image_processor import ImageProcessor, BatchImageProcessor
from classification_system.csv_writer import CSVWriter


def setup_logging(log_level: str = "INFO"):
    """Configura el sistema de logging."""
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        level=log_level,
    )
    logger.add(
        "data/logs/classification_{time}.log",
        rotation="500 MB",
        retention="10 days",
        level="DEBUG",
    )


def load_config(config_path: str = "config/classification_config.yaml") -> dict:
    """Carga configuración desde YAML."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


def process_video(video_path: str, detector: ObjectDetector, csv_writer: CSVWriter, config: dict):
    """
    Procesa un video completo.

    Args:
        video_path: Ruta al video
        detector: Detector YOLO
        csv_writer: Escritor CSV
        config: Configuración
    """
    logger.info(f"Procesando video: {video_path}")

    all_detections = []

    with VideoProcessor(
        video_path=video_path,
        frame_skip=config.get("frame_skip", 1),
        max_frames=config.get("max_frames"),
    ) as video_proc:

        for frame, source_info in video_proc.process_frames():
            # Detectar objetos en el frame
            detections = detector.detect(frame, source_info)
            all_detections.extend(detections)

        # Escribir todas las detecciones del video
        video_name = Path(video_path).stem
        csv_writer.write_video_detections(video_name, all_detections)

        # Escribir metadata del video
        metadata = video_proc.get_video_info()
        metadata["total_detections"] = len(all_detections)
        csv_writer.write_metadata(metadata, prefix=f"video_metadata_{video_name}")

    logger.info(f"Video procesado: {len(all_detections)} detecciones totales")


def process_images(image_paths: list, detector: ObjectDetector, csv_writer: CSVWriter):
    """
    Procesa múltiples imágenes.

    Args:
        image_paths: Lista de rutas a imágenes
        detector: Detector YOLO
        csv_writer: Escritor CSV
    """
    logger.info(f"Procesando {len(image_paths)} imágenes")

    batch_processor = BatchImageProcessor(image_paths)

    for image, source_info in batch_processor.process_all():
        # Detectar objetos
        detections = detector.detect(image, source_info)

        # Escribir detecciones de la imagen
        image_name = source_info["filename"]
        csv_writer.write_image_detections(image_name, detections)

    logger.info("Procesamiento de imágenes completado")


def main():
    """Función principal."""
    parser = argparse.ArgumentParser(description="Sistema de Clasificación YOLO")
    parser.add_argument(
        "--config",
        type=str,
        default="config/classification_config.yaml",
        help="Ruta al archivo de configuración",
    )
    parser.add_argument(
        "--video",
        type=str,
        help="Ruta a un video para procesar",
    )
    parser.add_argument(
        "--images",
        type=str,
        help="Directorio con imágenes para procesar",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Buscar imágenes recursivamente",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nivel de logging",
    )

    args = parser.parse_args()

    # Configurar logging
    setup_logging(args.log_level)

    logger.info("=== Iniciando Sistema de Clasificación ===")

    # Cargar configuración
    try:
        config = load_config(args.config)
        logger.info(f"Configuración cargada desde {args.config}")
    except Exception as e:
        logger.error(f"Error cargando configuración: {e}")
        sys.exit(1)

    # Inicializar detector
    try:
        detector = ObjectDetector(
            model_path=config.get("model_path", "yolov8n.pt"),
            confidence=config.get("confidence_threshold", 0.25),
        )
        model_info = detector.get_model_info()
        logger.info(f"Modelo: {model_info['num_classes']} clases")
    except Exception as e:
        logger.error(f"Error inicializando detector: {e}")
        sys.exit(1)

    # Inicializar CSV writer
    csv_writer = CSVWriter(
        staging_dir=config.get("staging_dir", "data/staging"),
        buffer_size=config.get("buffer_size", 100),
    )

    try:
        # Procesar video si se especificó
        if args.video:
            process_video(args.video, detector, csv_writer, config)

        # Procesar imágenes si se especificó
        if args.images:
            image_processors = ImageProcessor.batch_process_directory(
                args.images, recursive=args.recursive
            )
            image_paths = [str(p.image_path) for p in image_processors]
            process_images(image_paths, detector, csv_writer)

        # Si no se especificó entrada, procesar desde config
        if not args.video and not args.images:
            # Procesar videos de la configuración
            videos = config.get("input_videos", [])
            for video_path in videos:
                if Path(video_path).exists():
                    process_video(video_path, detector, csv_writer, config)
                else:
                    logger.warning(f"Video no encontrado: {video_path}")

            # Procesar imágenes de la configuración
            images_dir = config.get("input_images_dir")
            if images_dir and Path(images_dir).exists():
                image_processors = ImageProcessor.batch_process_directory(
                    images_dir, recursive=config.get("recursive_images", False)
                )
                image_paths = [str(p.image_path) for p in image_processors]
                process_images(image_paths, detector, csv_writer)

        # Resumen final
        summary = csv_writer.get_staging_summary()
        logger.info("=== Resumen de Procesamiento ===")
        logger.info(f"Archivos CSV generados: {summary['csv_files']}")
        logger.info(f"Detecciones totales: {summary['total_detections']}")
        logger.info(f"Staging directory: {summary['staging_directory']}")

    except Exception as e:
        logger.exception(f"Error durante el procesamiento: {e}")
        sys.exit(1)

    logger.info("=== Sistema de Clasificación Finalizado ===")


if __name__ == "__main__":
    main()