"""
Escritor de detecciones a archivos CSV (staging layer).
"""

import csv
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
from loguru import logger
import json


class CSVWriter:
    """Escribe detecciones en archivos CSV para el staging area."""

    def __init__(self, staging_dir: str, buffer_size: int = 100):
        """
        Inicializa el escritor CSV.

        Args:
            staging_dir: Directorio para archivos CSV
            buffer_size: Número de detecciones a acumular antes de escribir
        """
        self.staging_dir = Path(staging_dir)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.buffer_size = buffer_size
        self.buffer: List[Dict[str, Any]] = []
        self.files_created: List[Path] = []

        logger.info(f"CSV Writer inicializado. Staging dir: {self.staging_dir}")

    def add_detections(self, detections: List[Dict[str, Any]]):
        """
        Añade detecciones al buffer.

        Args:
            detections: Lista de diccionarios con detecciones
        """
        self.buffer.extend(detections)
        logger.debug(f"Buffer: {len(self.buffer)} detecciones")

        # Si el buffer alcanza el tamaño límite, escribir
        if len(self.buffer) >= self.buffer_size:
            self.flush()

    def flush(self, source_name: str = None):
        """
        Escribe el buffer a un archivo CSV.

        Args:
            source_name: Nombre opcional para el archivo (sin extensión)
        """
        if not self.buffer:
            logger.debug("Buffer vacío, no hay nada que escribir")
            return

        # Generar nombre de archivo
        if source_name:
            filename = f"{source_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        else:
            filename = f"detections_{datetime.now().strftime('%Y%m%d_%H%M%S%f')}.csv"

        filepath = self.staging_dir / filename

        # Obtener fieldnames del primer elemento
        if self.buffer:
            fieldnames = list(self.buffer[0].keys())

            try:
                with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(self.buffer)

                logger.info(f"Escritas {len(self.buffer)} detecciones en {filepath.name}")
                self.files_created.append(filepath)

                # Limpiar buffer
                self.buffer.clear()

            except Exception as e:
                logger.error(f"Error escribiendo CSV {filepath}: {e}")
                raise

    def write_video_detections(
        self, video_name: str, all_detections: List[Dict[str, Any]]
    ):
        """
        Escribe todas las detecciones de un video en un único archivo.

        Args:
            video_name: Nombre del video (sin extensión)
            all_detections: Lista completa de detecciones
        """
        if not all_detections:
            logger.warning(f"No hay detecciones para el video {video_name}")
            return

        # Limpiar nombre de archivo
        clean_name = Path(video_name).stem
        filename = f"video_{clean_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = self.staging_dir / filename

        fieldnames = list(all_detections[0].keys())

        try:
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_detections)

            logger.info(
                f"Escritas {len(all_detections)} detecciones de video en {filepath.name}"
            )
            self.files_created.append(filepath)

        except Exception as e:
            logger.error(f"Error escribiendo detecciones de video: {e}")
            raise

    def write_image_detections(
        self, image_name: str, detections: List[Dict[str, Any]]
    ):
        """
        Escribe detecciones de una imagen individual.

        Args:
            image_name: Nombre de la imagen
            detections: Detecciones de la imagen
        """
        if not detections:
            logger.warning(f"No hay detecciones para la imagen {image_name}")
            return

        clean_name = Path(image_name).stem
        filename = f"image_{clean_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = self.staging_dir / filename

        fieldnames = list(detections[0].keys())

        try:
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(detections)

            logger.info(
                f"Escritas {len(detections)} detecciones de imagen en {filepath.name}"
            )
            self.files_created.append(filepath)

        except Exception as e:
            logger.error(f"Error escribiendo detecciones de imagen: {e}")
            raise

    def write_metadata(self, metadata: Dict[str, Any], prefix: str = "metadata"):
        """
        Escribe metadata adicional en formato JSON.

        Args:
            metadata: Diccionario con metadata
            prefix: Prefijo para el nombre del archivo
        """
        filename = f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = self.staging_dir / filename

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, default=str)

            logger.info(f"Metadata escrita en {filepath.name}")

        except Exception as e:
            logger.error(f"Error escribiendo metadata: {e}")

    def get_created_files(self) -> List[Path]:
        """
        Obtiene la lista de archivos creados.

        Returns:
            Lista de rutas a archivos CSV creados
        """
        return self.files_created.copy()

    def get_staging_summary(self) -> Dict[str, Any]:
        """
        Obtiene resumen de archivos en staging.

        Returns:
            Diccionario con estadísticas
        """
        csv_files = list(self.staging_dir.glob("*.csv"))
        json_files = list(self.staging_dir.glob("*.json"))

        total_detections = 0
        for csv_file in csv_files:
            try:
                with open(csv_file, "r", encoding="utf-8") as f:
                    # Contar líneas (excluyendo header)
                    total_detections += sum(1 for _ in f) - 1
            except Exception as e:
                logger.warning(f"Error leyendo {csv_file}: {e}")

        return {
            "staging_directory": str(self.staging_dir),
            "csv_files": len(csv_files),
            "json_files": len(json_files),
            "total_detections": total_detections,
            "files": [f.name for f in csv_files],
        }

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - flush remaining buffer."""
        if self.buffer:
            logger.info("Flushing remaining buffer on exit")
            self.flush()