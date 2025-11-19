"""
Módulo de extracción: Lee archivos CSV del staging area.
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from loguru import logger
import re


class DataExtractor:
    """Extrae datos de archivos CSV en el staging area."""

    def __init__(self, staging_dir: str):
        """
        Inicializa el extractor.

        Args:
            staging_dir: Directorio de staging con archivos CSV
        """
        self.staging_dir = Path(staging_dir)
        if not self.staging_dir.exists():
            raise FileNotFoundError(f"Staging directory no existe: {staging_dir}")

        logger.info(f"Extractor inicializado para: {self.staging_dir}")

    def scan_staging_files(self) -> Dict[str, List[Path]]:
        """
        Escanea el staging area y clasifica archivos por tipo.

        Returns:
            Diccionario con listas de archivos por tipo (video, image)
        """
        csv_files = list(self.staging_dir.glob("*.csv"))

        categorized = {"video": [], "image": [], "unknown": []}

        for file in csv_files:
            if file.name.startswith("video_"):
                categorized["video"].append(file)
            elif file.name.startswith("image_"):
                categorized["image"].append(file)
            else:
                categorized["unknown"].append(file)

        logger.info(
            f"Archivos encontrados - Videos: {len(categorized['video'])}, "
            f"Imágenes: {len(categorized['image'])}, "
            f"Desconocidos: {len(categorized['unknown'])}"
        )

        return categorized

    def extract_csv(self, filepath: Path) -> pd.DataFrame:
        """
        Extrae datos de un archivo CSV.

        Args:
            filepath: Ruta al archivo CSV

        Returns:
            DataFrame con los datos
        """
        try:
            df = pd.read_csv(filepath)
            logger.info(f"Extraídas {len(df)} filas de {filepath.name}")

            # Añadir metadata de extracción
            df["_extracted_at"] = datetime.now().isoformat()
            df["_source_file"] = filepath.name

            return df

        except Exception as e:
            logger.error(f"Error extrayendo {filepath}: {e}")
            raise

    def extract_multiple(self, filepaths: List[Path]) -> pd.DataFrame:
        """
        Extrae y combina múltiples archivos CSV.

        Args:
            filepaths: Lista de rutas a archivos

        Returns:
            DataFrame combinado
        """
        dataframes = []

        for filepath in filepaths:
            try:
                df = self.extract_csv(filepath)
                dataframes.append(df)
            except Exception as e:
                logger.error(f"Error procesando {filepath}: {e}")
                continue

        if not dataframes:
            logger.warning("No se pudieron extraer datos de ningún archivo")
            return pd.DataFrame()

        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Combinadas {len(dataframes)} fuentes: {len(combined_df)} filas totales")

        return combined_df

    def extract_by_time_window(
        self, start_time: datetime, end_time: datetime, source_type: str = "video"
    ) -> pd.DataFrame:
        """
        Extrae archivos dentro de una ventana de tiempo.

        Args:
            start_time: Tiempo inicial
            end_time: Tiempo final
            source_type: Tipo de fuente ('video' o 'image')

        Returns:
            DataFrame con datos del período
        """
        files = self.scan_staging_files()[source_type]

        selected_files = []
        for file in files:
            # Extraer timestamp del nombre del archivo
            match = re.search(r"(\d{8}_\d{6})", file.name)
            if match:
                try:
                    file_time = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
                    if start_time <= file_time <= end_time:
                        selected_files.append(file)
                except ValueError:
                    logger.warning(f"No se pudo parsear timestamp de {file.name}")

        logger.info(
            f"Seleccionados {len(selected_files)} archivos entre "
            f"{start_time} y {end_time}"
        )

        return self.extract_multiple(selected_files)

    def extract_pending_files(
        self, processed_files: List[str], source_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Extrae archivos que aún no han sido procesados.

        Args:
            processed_files: Lista de nombres de archivos ya procesados
            source_type: Tipo de fuente o None para todos

        Returns:
            DataFrame con datos pendientes
        """
        if source_type:
            all_files = self.scan_staging_files()[source_type]
        else:
            categorized = self.scan_staging_files()
            all_files = categorized["video"] + categorized["image"]

        pending_files = [f for f in all_files if f.name not in processed_files]

        logger.info(f"Archivos pendientes: {len(pending_files)}")

        return self.extract_multiple(pending_files)

    def get_file_metadata(self, filepath: Path) -> Dict[str, Any]:
        """
        Obtiene metadata de un archivo CSV.

        Args:
            filepath: Ruta al archivo

        Returns:
            Diccionario con metadata
        """
        stat = filepath.stat()

        # Contar filas sin cargar todo el archivo
        with open(filepath, "r") as f:
            row_count = sum(1 for _ in f) - 1  # Excluir header

        # Extraer timestamp del nombre
        match = re.search(r"(\d{8}_\d{6})", filepath.name)
        file_timestamp = None
        if match:
            try:
                file_timestamp = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
            except ValueError:
                pass

        return {
            "filename": filepath.name,
            "filepath": str(filepath),
            "size_bytes": stat.st_size,
            "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
            "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "row_count": row_count,
            "file_timestamp": file_timestamp.isoformat() if file_timestamp else None,
        }

    def get_staging_summary(self) -> Dict[str, Any]:
        """
        Obtiene resumen completo del staging area.

        Returns:
            Diccionario con estadísticas
        """
        categorized = self.scan_staging_files()

        summary = {
            "staging_directory": str(self.staging_dir),
            "total_files": sum(len(files) for files in categorized.values()),
            "by_type": {k: len(v) for k, v in categorized.items()},
            "files": {},
        }

        for file_type, files in categorized.items():
            summary["files"][file_type] = [
                self.get_file_metadata(f) for f in files[:10]  # Limitar a 10
            ]

        return summary