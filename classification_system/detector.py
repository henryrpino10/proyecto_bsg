"""
Módulo de detección de objetos usando YOLO.
Extrae atributos ricos de cada detección.
"""

from typing import List, Dict, Any
import numpy as np
from ultralytics import YOLO
from loguru import logger
import hashlib
from datetime import datetime


class ObjectDetector:
    """Clase para detección de objetos con YOLO."""

    def __init__(self, model_path: str = "yolov8n.pt", confidence: float = 0.25):
        """
        Inicializa el detector YOLO.

        Args:
            model_path: Ruta al modelo YOLO (por defecto YOLOv8 nano)
            confidence: Umbral de confianza para detecciones
        """
        logger.info(f"Cargando modelo YOLO desde {model_path}")
        self.model = YOLO(model_path)
        self.confidence = confidence
        logger.info(f"Modelo cargado. Umbral de confianza: {confidence}")

    def detect(
        self, image: np.ndarray, source_info: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Realiza detección de objetos en una imagen.

        Args:
            image: Imagen en formato numpy array (BGR)
            source_info: Información de la fuente (archivo, frame, timestamp, etc.)

        Returns:
            Lista de diccionarios con atributos de cada detección
        """
        results = self.model(image, conf=self.confidence, verbose=False)

        detections = []
        for result in results:
            boxes = result.boxes

            for idx, box in enumerate(boxes):
                detection = self._extract_attributes(
                    box=box,
                    image_shape=image.shape,
                    source_info=source_info,
                    detection_idx=idx,
                )
                detections.append(detection)

        logger.debug(
            f"Detectados {len(detections)} objetos en {source_info.get('source', 'unknown')}"
        )
        return detections

    def _extract_attributes(
        self,
        box: Any,
        image_shape: tuple,
        source_info: Dict[str, Any],
        detection_idx: int,
    ) -> Dict[str, Any]:
        """
        Extrae atributos ricos de una detección.

        Args:
            box: Objeto box de YOLO con información de la detección
            image_shape: Dimensiones de la imagen (height, width, channels)
            source_info: Información de la fuente
            detection_idx: Índice de la detección en el frame

        Returns:
            Diccionario con todos los atributos de la detección
        """
        # Extraer coordenadas del bounding box
        x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
        width = x2 - x1
        height = y2 - y1
        area = width * height

        # Información de la clase
        class_id = int(box.cls[0].cpu().numpy())
        class_name = self.model.names[class_id]
        confidence = float(box.conf[0].cpu().numpy())

        # Calcular centro y normalización
        center_x = (x1 + x2) / 2
        center_y = (y1 + y2) / 2
        img_height, img_width = image_shape[:2]

        # Calcular aspect ratio
        aspect_ratio = width / height if height > 0 else 0

        # Generar ID único para la detección
        detection_id = self._generate_detection_id(source_info, detection_idx)

        detection = {
            # Identificación
            "detection_id": detection_id,
            "source_type": source_info.get("type", "unknown"),
            "source_file": source_info.get("source", "unknown"),
            "timestamp": source_info.get("timestamp", datetime.now().isoformat()),
            # Frame info (para videos)
            "frame_number": source_info.get("frame_number"),
            "frame_timestamp": source_info.get("frame_timestamp"),
            # Clase y confianza
            "class_id": class_id,
            "class_name": class_name,
            "confidence": round(confidence, 4),
            # Bounding box (coordenadas absolutas)
            "bbox_x1": round(x1, 2),
            "bbox_y1": round(y1, 2),
            "bbox_x2": round(x2, 2),
            "bbox_y2": round(y2, 2),
            # Dimensiones del box
            "bbox_width": round(width, 2),
            "bbox_height": round(height, 2),
            "bbox_area": round(area, 2),
            # Posición del centro
            "center_x": round(center_x, 2),
            "center_y": round(center_y, 2),
            # Coordenadas normalizadas (0-1)
            "normalized_x1": round(x1 / img_width, 4),
            "normalized_y1": round(y1 / img_height, 4),
            "normalized_x2": round(x2 / img_width, 4),
            "normalized_y2": round(y2 / img_height, 4),
            # Métricas adicionales
            "aspect_ratio": round(aspect_ratio, 4),
            "relative_area": round(area / (img_width * img_height), 6),
            # Dimensiones de la imagen
            "image_width": img_width,
            "image_height": img_height,
            # Índice de detección
            "detection_index": detection_idx,
        }

        return detection

    @staticmethod
    def _generate_detection_id(source_info: Dict[str, Any], detection_idx: int) -> str:
        """
        Genera un ID único para la detección.

        Args:
            source_info: Información de la fuente
            detection_idx: Índice de la detección

        Returns:
            ID único basado en hash
        """
        # Crear string único combinando información relevante
        unique_string = (
            f"{source_info.get('source', '')}:"
            f"{source_info.get('frame_number', 0)}:"
            f"{source_info.get('timestamp', '')}:"
            f"{detection_idx}"
        )

        # Generar hash MD5
        hash_object = hashlib.md5(unique_string.encode())
        return hash_object.hexdigest()

    def get_model_info(self) -> Dict[str, Any]:
        """
        Obtiene información sobre el modelo cargado.

        Returns:
            Diccionario con información del modelo
        """
        return {
            "model_type": self.model.task,
            "model_name": type(self.model).__name__,
            "num_classes": len(self.model.names),
            "class_names": self.model.names,
            "confidence_threshold": self.confidence,
        }