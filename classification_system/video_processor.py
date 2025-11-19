"""
Procesador de videos para detección frame por frame.
"""

import cv2
from pathlib import Path
from typing import Generator, Dict, Any, Optional
from datetime import datetime, timedelta
from loguru import logger
from tqdm import tqdm


class VideoProcessor:
    """Procesa videos frame por frame para detección de objetos."""

    def __init__(
        self,
        video_path: str,
        frame_skip: int = 1,
        max_frames: Optional[int] = None,
    ):
        """
        Inicializa el procesador de video.

        Args:
            video_path: Ruta al archivo de video
            frame_skip: Procesar 1 de cada N frames (1 = todos los frames)
            max_frames: Máximo número de frames a procesar (None = todos)
        """
        self.video_path = Path(video_path)
        self.frame_skip = frame_skip
        self.max_frames = max_frames

        if not self.video_path.exists():
            raise FileNotFoundError(f"Video no encontrado: {video_path}")

        self.cap = cv2.VideoCapture(str(self.video_path))
        if not self.cap.isOpened():
            raise ValueError(f"No se puede abrir el video: {video_path}")

        # Obtener información del video
        self.fps = self.cap.get(cv2.CAP_PROP_FPS)
        self.total_frames = int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))
        self.width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        self.height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        self.duration = self.total_frames / self.fps if self.fps > 0 else 0

        logger.info(f"Video cargado: {self.video_path.name}")
        logger.info(f"  Resolución: {self.width}x{self.height}")
        logger.info(f"  FPS: {self.fps:.2f}")
        logger.info(f"  Frames totales: {self.total_frames}")
        logger.info(f"  Duración: {self.duration:.2f}s")
        logger.info(f"  Frame skip: {self.frame_skip}")

    def process_frames(self) -> Generator[tuple, None, None]:
        """
        Generador que produce frames del video con su información.

        Yields:
            Tupla (frame, source_info) donde:
                - frame: numpy array con el frame
                - source_info: diccionario con metadata del frame
        """
        frame_count = 0
        processed_count = 0
        start_time = datetime.now()

        # Calcular frames a procesar
        frames_to_process = min(
            self.total_frames // self.frame_skip,
            self.max_frames if self.max_frames else float("inf"),
        )

        with tqdm(total=int(frames_to_process), desc="Procesando video") as pbar:
            while True:
                ret, frame = self.cap.read()

                if not ret:
                    break

                # Aplicar frame skip
                if frame_count % self.frame_skip == 0:
                    # Calcular timestamp del frame
                    frame_timestamp = frame_count / self.fps if self.fps > 0 else 0

                    source_info = {
                        "type": "video",
                        "source": str(self.video_path),
                        "filename": self.video_path.name,
                        "timestamp": start_time.isoformat(),
                        "frame_number": frame_count,
                        "frame_timestamp": round(frame_timestamp, 3),
                        "fps": self.fps,
                        "video_width": self.width,
                        "video_height": self.height,
                        "total_frames": self.total_frames,
                    }

                    yield frame, source_info

                    processed_count += 1
                    pbar.update(1)

                    # Verificar límite máximo de frames
                    if self.max_frames and processed_count >= self.max_frames:
                        logger.info(f"Alcanzado límite de {self.max_frames} frames")
                        break

                frame_count += 1

        logger.info(
            f"Video procesado: {processed_count} de {self.total_frames} frames"
        )

    def get_video_info(self) -> Dict[str, Any]:
        """
        Obtiene información completa del video.

        Returns:
            Diccionario con metadata del video
        """
        return {
            "path": str(self.video_path),
            "filename": self.video_path.name,
            "width": self.width,
            "height": self.height,
            "fps": self.fps,
            "total_frames": self.total_frames,
            "duration_seconds": self.duration,
            "frame_skip": self.frame_skip,
            "max_frames": self.max_frames,
        }

    def get_frame_at_time(self, time_seconds: float) -> Optional[tuple]:
        """
        Obtiene un frame específico del video por timestamp.

        Args:
            time_seconds: Tiempo en segundos

        Returns:
            Tupla (frame, source_info) o None si no se puede obtener
        """
        frame_number = int(time_seconds * self.fps)

        if frame_number >= self.total_frames:
            logger.warning(f"Frame {frame_number} fuera de rango")
            return None

        self.cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
        ret, frame = self.cap.read()

        if not ret:
            logger.error(f"Error al leer frame {frame_number}")
            return None

        source_info = {
            "type": "video",
            "source": str(self.video_path),
            "filename": self.video_path.name,
            "timestamp": datetime.now().isoformat(),
            "frame_number": frame_number,
            "frame_timestamp": time_seconds,
            "fps": self.fps,
            "video_width": self.width,
            "video_height": self.height,
        }

        return frame, source_info

    def close(self):
        """Libera recursos del video."""
        if self.cap:
            self.cap.release()
            logger.debug(f"Video cerrado: {self.video_path.name}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def __del__(self):
        """Destructor."""
        self.close()