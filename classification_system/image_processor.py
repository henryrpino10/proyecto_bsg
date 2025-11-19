"""
Procesador de imágenes para detección de objetos.
"""

import cv2
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
from loguru import logger
from PIL import Image
import hashlib


class ImageProcessor:
    """Procesa imágenes individuales para detección de objetos."""

    SUPPORTED_FORMATS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp"}

    def __init__(self, image_path: str):
        """
        Inicializa el procesador de imagen.

        Args:
            image_path: Ruta al archivo de imagen
        """
        self.image_path = Path(image_path)

        if not self.image_path.exists():
            raise FileNotFoundError(f"Imagen no encontrada: {image_path}")

        if self.image_path.suffix.lower() not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Formato no soportado: {self.image_path.suffix}. "
                f"Formatos válidos: {self.SUPPORTED_FORMATS}"
            )

        # Cargar imagen
        self.image = cv2.imread(str(self.image_path))
        if self.image is None:
            raise ValueError(f"No se puede cargar la imagen: {image_path}")

        self.height, self.width = self.image.shape[:2]

        logger.info(f"Imagen cargada: {self.image_path.name}")
        logger.info(f"  Resolución: {self.width}x{self.height}")

    def get_image_and_info(self) -> tuple:
        """
        Obtiene la imagen y su información.

        Returns:
            Tupla (image, source_info) donde:
                - image: numpy array con la imagen
                - source_info: diccionario con metadata
        """
        source_info = {
            "type": "image",
            "source": str(self.image_path),
            "filename": self.image_path.name,
            "timestamp": datetime.now().isoformat(),
            "image_width": self.width,
            "image_height": self.height,
            "file_size_bytes": self.image_path.stat().st_size,
            "format": self.image_path.suffix.lower(),
            "file_hash": self._calculate_file_hash(),
        }

        return self.image, source_info

    def _calculate_file_hash(self) -> str:
        """
        Calcula hash MD5 del archivo para identificación única.

        Returns:
            Hash MD5 del archivo
        """
        hash_md5 = hashlib.md5()
        with open(self.image_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def get_image_metadata(self) -> Dict[str, Any]:
        """
        Obtiene metadata extendida de la imagen usando PIL.

        Returns:
            Diccionario con metadata EXIF si está disponible
        """
        try:
            img_pil = Image.open(self.image_path)
            metadata = {
                "format": img_pil.format,
                "mode": img_pil.mode,
                "size": img_pil.size,
            }

            # Intentar obtener datos EXIF
            exif_data = img_pil.getexif()
            if exif_data:
                metadata["exif"] = dict(exif_data)

            return metadata
        except Exception as e:
            logger.warning(f"No se pudo obtener metadata EXIF: {e}")
            return {}

    @staticmethod
    def batch_process_directory(
        directory: str, recursive: bool = False
    ) -> List["ImageProcessor"]:
        """
        Crea procesadores para todas las imágenes en un directorio.

        Args:
            directory: Ruta al directorio
            recursive: Si True, busca en subdirectorios

        Returns:
            Lista de ImageProcessor
        """
        directory_path = Path(directory)
        if not directory_path.exists():
            raise FileNotFoundError(f"Directorio no encontrado: {directory}")

        processors = []
        pattern = "**/*" if recursive else "*"

        for file_path in directory_path.glob(pattern):
            if file_path.suffix.lower() in ImageProcessor.SUPPORTED_FORMATS:
                try:
                    processor = ImageProcessor(str(file_path))
                    processors.append(processor)
                except Exception as e:
                    logger.error(f"Error procesando {file_path}: {e}")

        logger.info(f"Encontradas {len(processors)} imágenes en {directory}")
        return processors


class BatchImageProcessor:
    """Procesador de lotes de imágenes."""

    def __init__(self, image_paths: List[str]):
        """
        Inicializa el procesador de lote.

        Args:
            image_paths: Lista de rutas a imágenes
        """
        self.image_paths = image_paths
        self.total_images = len(image_paths)
        logger.info(f"Lote de {self.total_images} imágenes preparado")

    def process_all(self):
        """
        Generador que procesa todas las imágenes del lote.

        Yields:
            Tupla (image, source_info) para cada imagen válida
        """
        from tqdm import tqdm

        successful = 0
        failed = 0

        for img_path in tqdm(self.image_paths, desc="Procesando imágenes"):
            try:
                processor = ImageProcessor(img_path)
                image, source_info = processor.get_image_and_info()
                yield image, source_info
                successful += 1
            except Exception as e:
                logger.error(f"Error procesando {img_path}: {e}")
                failed += 1

        logger.info(
            f"Procesamiento completado: {successful} exitosas, {failed} fallidas"
        )