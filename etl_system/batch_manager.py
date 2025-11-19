"""
Gestor de lotes: Controla reglas de tiempo y finalización para envío a Hive.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from loguru import logger
import json
import time


class BatchManager:
    """
    Gestiona lotes de datos según reglas de tiempo (video) y finalización (imagen).
    """

    def __init__(
        self,
        video_time_window: int = 300,  # 5 minutos en segundos
        image_batch_size: int = 100,  # Número de imágenes por lote
        state_file: str = "data/etl_state.json",
    ):
        """
        Inicializa el gestor de lotes.

        Args:
            video_time_window: Ventana de tiempo para videos (segundos)
            image_batch_size: Número de imágenes para formar un lote
            state_file: Archivo para persistir estado del ETL
        """
        self.video_time_window = video_time_window
        self.image_batch_size = image_batch_size
        self.state_file = Path(state_file)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

        self.state = self._load_state()

        logger.info(f"BatchManager inicializado")
        logger.info(f"  Ventana de tiempo para videos: {video_time_window}s")
        logger.info(f"  Tamaño de lote para imágenes: {image_batch_size}")

    def _load_state(self) -> Dict[str, Any]:
        """
        Carga el estado persistido del ETL.

        Returns:
            Diccionario con estado
        """
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    state = json.load(f)
                logger.info("Estado cargado desde archivo")
                return state
            except Exception as e:
                logger.warning(f"Error cargando estado: {e}, usando estado vacío")

        return {
            "processed_files": [],
            "last_video_batch": None,
            "pending_images": [],
            "last_run": None,
            "statistics": {
                "total_batches": 0,
                "total_records": 0,
                "video_batches": 0,
                "image_batches": 0,
            },
        }

    def _save_state(self):
        """Persiste el estado actual."""
        try:
            with open(self.state_file, "w") as f:
                json.dump(self.state, f, indent=2, default=str)
            logger.debug("Estado guardado")
        except Exception as e:
            logger.error(f"Error guardando estado: {e}")

    def should_process_video_batch(self) -> bool:
        """
        Determina si es momento de procesar un lote de videos (regla de tiempo).

        Returns:
            True si se debe procesar
        """
        if self.state["last_video_batch"] is None:
            return True

        last_batch = datetime.fromisoformat(self.state["last_video_batch"])
        time_elapsed = (datetime.now() - last_batch).total_seconds()

        should_process = time_elapsed >= self.video_time_window

        if should_process:
            logger.info(
                f"Ventana de tiempo alcanzada ({time_elapsed:.1f}s >= {self.video_time_window}s)"
            )

        return should_process

    def should_process_image_batch(self, pending_count: int) -> bool:
        """
        Determina si hay suficientes imágenes para formar un lote (regla de finalización).

        Args:
            pending_count: Número de imágenes pendientes

        Returns:
            True si se debe procesar
        """
        should_process = pending_count >= self.image_batch_size

        if should_process:
            logger.info(
                f"Lote de imágenes completo ({pending_count} >= {self.image_batch_size})"
            )

        return should_process

    def mark_files_processed(self, filenames: List[str]):
        """
        Marca archivos como procesados.

        Args:
            filenames: Lista de nombres de archivo
        """
        for filename in filenames:
            if filename not in self.state["processed_files"]:
                self.state["processed_files"].append(filename)

        logger.info(f"Marcados {len(filenames)} archivos como procesados")
        self._save_state()

    def update_video_batch_time(self):
        """Actualiza el timestamp del último lote de video procesado."""
        self.state["last_video_batch"] = datetime.now().isoformat()
        self.state["statistics"]["video_batches"] += 1
        self._save_state()

    def update_image_batch_processed(self, count: int):
        """
        Actualiza estadísticas de procesamiento de imágenes.

        Args:
            count: Número de imágenes procesadas
        """
        self.state["statistics"]["image_batches"] += 1
        self.state["pending_images"] = []
        self._save_state()

    def update_statistics(self, records_processed: int):
        """
        Actualiza estadísticas generales.

        Args:
            records_processed: Número de registros procesados
        """
        self.state["statistics"]["total_batches"] += 1
        self.state["statistics"]["total_records"] += records_processed
        self.state["last_run"] = datetime.now().isoformat()
        self._save_state()

    def get_pending_files(
        self, all_files: List[str], source_type: Optional[str] = None
    ) -> List[str]:
        """
        Obtiene archivos que aún no han sido procesados.

        Args:
            all_files: Lista de todos los archivos disponibles
            source_type: Filtrar por tipo ('video' o 'image')

        Returns:
            Lista de archivos pendientes
        """
        processed = set(self.state["processed_files"])
        pending = [f for f in all_files if f not in processed]

        if source_type:
            pending = [f for f in pending if f.startswith(f"{source_type}_")]

        logger.info(f"Archivos pendientes ({source_type or 'todos'}): {len(pending)}")
        return pending

    def get_statistics(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas del gestor de lotes.

        Returns:
            Diccionario con estadísticas
        """
        stats = self.state["statistics"].copy()

        # Calcular tiempo desde última ejecución
        if self.state["last_run"]:
            last_run = datetime.fromisoformat(self.state["last_run"])
            stats["minutes_since_last_run"] = (
                datetime.now() - last_run
            ).total_seconds() / 60

        # Tiempo hasta próximo lote de video
        if self.state["last_video_batch"]:
            last_batch = datetime.fromisoformat(self.state["last_video_batch"])
            elapsed = (datetime.now() - last_batch).total_seconds()
            stats["seconds_until_next_video_batch"] = max(
                0, self.video_time_window - elapsed
            )

        stats["processed_files_count"] = len(self.state["processed_files"])

        return stats

    def reset_state(self):
        """Resetea completamente el estado (usar con precaución)."""
        logger.warning("Reseteando estado del BatchManager")
        self.state = {
            "processed_files": [],
            "last_video_batch": None,
            "pending_images": [],
            "last_run": None,
            "statistics": {
                "total_batches": 0,
                "total_records": 0,
                "video_batches": 0,
                "image_batches": 0,
            },
        }
        self._save_state()

    def cleanup_old_state(self, days_to_keep: int = 7):
        """
        Limpia archivos procesados antiguos del estado.

        Args:
            days_to_keep: Días de historia a mantener
        """
        # Esta función podría mejorar filtrando por timestamp
        # Por ahora solo limita el tamaño de la lista
        max_files = 10000

        if len(self.state["processed_files"]) > max_files:
            logger.info(f"Limpiando lista de archivos procesados")
            self.state["processed_files"] = self.state["processed_files"][
                -max_files:
            ]
            self._save_state()


class BatchScheduler:
    """Planificador para ejecución periódica del ETL."""

    def __init__(self, batch_manager: BatchManager, check_interval: int = 60):
        """
        Inicializa el scheduler.

        Args:
            batch_manager: Instancia de BatchManager
            check_interval: Intervalo de verificación (segundos)
        """
        self.batch_manager = batch_manager
        self.check_interval = check_interval
        self.running = False

        logger.info(f"Scheduler inicializado - Intervalo: {check_interval}s")

    def run(self, etl_callback):
        """
        Ejecuta el scheduler en loop continuo.

        Args:
            etl_callback: Función callback para ejecutar ETL
        """
        self.running = True
        logger.info("Scheduler iniciado")

        while self.running:
            try:
                # Verificar si hay lotes para procesar
                if self.batch_manager.should_process_video_batch():
                    logger.info("Ejecutando ETL para videos")
                    etl_callback(source_type="video")

                # Verificar estadísticas de imágenes pendientes
                stats = self.batch_manager.get_statistics()
                pending_images = len(self.batch_manager.state.get("pending_images", []))

                if self.batch_manager.should_process_image_batch(pending_images):
                    logger.info("Ejecutando ETL para imágenes")
                    etl_callback(source_type="image")

                # Esperar hasta próxima verificación
                logger.debug(f"Esperando {self.check_interval}s hasta próxima verificación")
                time.sleep(self.check_interval)

            except KeyboardInterrupt:
                logger.info("Scheduler detenido por usuario")
                self.running = False
            except Exception as e:
                logger.error(f"Error en scheduler: {e}")
                time.sleep(self.check_interval)

    def stop(self):
        """Detiene el scheduler."""
        logger.info("Deteniendo scheduler")
        self.running = False