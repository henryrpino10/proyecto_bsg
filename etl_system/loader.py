"""
Módulo de carga: Envía datos procesados a Apache Hive.
"""

import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime
from loguru import logger
from pyhive import hive
from contextlib import contextmanager
from pathlib import Path
import tempfile


class HiveLoader:
    """Carga datos procesados en Apache Hive."""

    def __init__(self, host: str = "localhost", port: int = 10000, database: str = "detections_db", username: str = "hive"):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        logger.info(f"HiveLoader configurado: {host}:{port}/{database}")

    @contextmanager
    def get_connection(self, use_database=True):
        conn = None
        try:
            if use_database:
                conn = hive.Connection(host=self.host, port=self.port, username=self.username, database=self.database)
            else:
                conn = hive.Connection(host=self.host, port=self.port, username=self.username)
            logger.debug("Conexión a Hive establecida")
            yield conn
        except Exception as e:
            logger.error(f"Error conectando a Hive: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("Conexión a Hive cerrada")

    def create_database(self):
        try:
            with self.get_connection(use_database=False) as conn:
                cursor = conn.cursor()
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
                logger.info(f"Base de datos {self.database} verificada/creada")
        except Exception as e:
            logger.error(f"Error creando base de datos: {e}")
            raise

    def create_detections_table(self, table_name: str = "detections"):
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.{table_name} (
            detection_id STRING,
            source_type STRING,
            source_file STRING,
            source_filename STRING,
            detection_timestamp STRING,
            frame_number INT,
            frame_timestamp DOUBLE,
            class_id INT,
            class_name STRING,
            confidence DOUBLE,
            bbox_x1 DOUBLE,
            bbox_y1 DOUBLE,
            bbox_x2 DOUBLE,
            bbox_y2 DOUBLE,
            bbox_width DOUBLE,
            bbox_height DOUBLE,
            bbox_area DOUBLE,
            center_x DOUBLE,
            center_y DOUBLE,
            normalized_x1 DOUBLE,
            normalized_y1 DOUBLE,
            normalized_x2 DOUBLE,
            normalized_y2 DOUBLE,
            aspect_ratio DOUBLE,
            relative_area DOUBLE,
            image_width INT,
            image_height INT,
            detection_index INT,
            size_category STRING,
            confidence_category STRING,
            horizontal_position STRING,
            vertical_position STRING,
            extracted_at STRING,
            processed_at STRING,
            loaded_at STRING,
            source_csv_file STRING,
            processing_date STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        TBLPROPERTIES ('skip.header.line.count'='1')
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(create_table_sql)
                logger.info(f"Tabla {table_name} creada/verificada")
        except Exception as e:
            logger.error(f"Error creando tabla: {e}")
            raise

    def load_batch(self, df: pd.DataFrame, table_name: str = "detections", batch_size: int = 1000) -> int:
        if df.empty:
            logger.warning("DataFrame vacío, no hay datos para cargar")
            return 0

        df = df.copy()
        df["loaded_at"] = datetime.now().isoformat()

        rename_map = {
            'timestamp': 'detection_timestamp',
            'filename': 'source_filename',
            '_extracted_at': 'extracted_at',
            '_processed_at': 'processed_at',
            '_loaded_at': 'loaded_at',
            '_source_file': 'source_csv_file'
        }
        
        for old_col, new_col in rename_map.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})

        if "detection_timestamp" in df.columns:
            df["processing_date"] = pd.to_datetime(df["detection_timestamp"]).dt.strftime("%Y-%m-%d")
        elif "timestamp" in df.columns:
            df["processing_date"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d")
        else:
            df["processing_date"] = datetime.now().strftime("%Y-%m-%d")

        expected_columns = [
            "detection_id", "source_type", "source_file", "source_filename",
            "detection_timestamp", "frame_number", "frame_timestamp", "class_id",
            "class_name", "confidence", "bbox_x1", "bbox_y1", "bbox_x2", "bbox_y2",
            "bbox_width", "bbox_height", "bbox_area", "center_x", "center_y",
            "normalized_x1", "normalized_y1", "normalized_x2", "normalized_y2",
            "aspect_ratio", "relative_area", "image_width", "image_height",
            "detection_index", "size_category", "confidence_category",
            "horizontal_position", "vertical_position", "extracted_at",
            "processed_at", "loaded_at", "source_csv_file", "processing_date"
        ]
        
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None
        
        df = df[expected_columns]
        logger.info(f"DataFrame preparado: {len(df)} filas, {len(df.columns)} columnas")

        total_loaded = 0

        try:
            # Crear archivo CSV temporal en el servidor de Hive
            temp_dir = Path("/tmp/hive_load")
            temp_dir.mkdir(exist_ok=True)
            
            temp_file = temp_dir / f"load_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            # Guardar DataFrame como CSV
            df.to_csv(temp_file, index=False, header=True, sep=',', quoting=1)
            logger.info(f"Archivo temporal creado: {temp_file}")
            
            # Cargar usando LOAD DATA
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Usar LOAD DATA LOCAL INPATH
                load_sql = f"LOAD DATA LOCAL INPATH '{temp_file}' INTO TABLE {self.database}.{table_name}"
                
                try:
                    cursor.execute(load_sql)
                    total_loaded = len(df)
                    logger.info(f"Carga completada: {total_loaded} filas")
                except Exception as e:
                    logger.error(f"Error con LOAD DATA, intentando método alternativo: {e}")
                    
                    # Método alternativo: Leer CSV y hacer INSERT por lotes
                    for start_idx in range(0, len(df), 100):
                        end_idx = min(start_idx + 100, len(df))
                        batch_df = df.iloc[start_idx:end_idx]
                        
                        values_rows = []
                        for _, row in batch_df.iterrows():
                            values = []
                            for val in row:
                                if pd.isna(val) or val is None:
                                    values.append("NULL")
                                elif isinstance(val, str):
                                    clean_val = str(val).replace("'", "''").replace('"', '""')
                                    values.append(f"'{clean_val}'")
                                else:
                                    values.append(str(val))
                            values_rows.append(f"({', '.join(values)})")
                        
                        # INSERT con múltiples valores
                        insert_sql = f"INSERT INTO {self.database}.{table_name} VALUES {', '.join(values_rows)}"
                        
                        try:
                            cursor.execute(insert_sql)
                            total_loaded += len(batch_df)
                            logger.debug(f"Lote insertado: {total_loaded}/{len(df)}")
                        except Exception as e2:
                            logger.warning(f"Error en lote: {e2}")
            
            # Limpiar archivo temporal
            try:
                temp_file.unlink()
            except:
                pass
                
        except Exception as e:
            logger.error(f"Error cargando datos: {e}")
            raise

        return total_loaded

    def load_with_deduplication(self, df: pd.DataFrame, table_name: str = "detections") -> int:
        if df.empty:
            return 0
        existing_ids = self.get_existing_detection_ids(table_name)
        df_new = df[~df["detection_id"].isin(existing_ids)]
        if df_new.empty:
            logger.info("Todos los registros ya existen en Hive")
            return 0
        duplicates_count = len(df) - len(df_new)
        logger.info(f"Registros únicos a cargar: {len(df_new)} (duplicados omitidos: {duplicates_count})")
        return self.load_batch(df_new, table_name)

    def get_existing_detection_ids(self, table_name: str = "detections") -> set:
        try:
            with self.get_connection() as conn:
                query = f"SELECT detection_id FROM {self.database}.{table_name}"
                df = pd.read_sql(query, conn)
                ids = set(df["detection_id"].values)
                logger.debug(f"IDs existentes en Hive: {len(ids)}")
                return ids
        except Exception as e:
            logger.warning(f"Error obteniendo IDs existentes: {e}")
            return set()

    def get_table_stats(self, table_name: str = "detections") -> Dict[str, Any]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {self.database}.{table_name}")
                total_rows = cursor.fetchone()[0]
                cursor.execute(f"SELECT source_type, COUNT(*) as count FROM {self.database}.{table_name} GROUP BY source_type")
                by_source = dict(cursor.fetchall())
                cursor.execute(f"SELECT class_name, COUNT(*) as count FROM {self.database}.{table_name} GROUP BY class_name ORDER BY count DESC LIMIT 10")
                top_classes = dict(cursor.fetchall())
                return {"table_name": table_name, "total_rows": total_rows, "by_source_type": by_source, "top_10_classes": top_classes}
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return {}

    def query_data(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        try:
            with self.get_connection() as conn:
                df = pd.read_sql(query, conn, params=params)
                logger.info(f"Query ejecutado: {len(df)} filas")
                return df
        except Exception as e:
            logger.error(f"Error ejecutando query: {e}")
            raise

    def cleanup_old_data(self, table_name: str = "detections", days_to_keep: int = 30):
        try:
            cutoff_date = (datetime.now() - pd.Timedelta(days=days_to_keep)).strftime("%Y-%m-%d")
            with self.get_connection() as conn:
                cursor = conn.cursor()
                delete_sql = f"DELETE FROM {self.database}.{table_name} WHERE processing_date < '{cutoff_date}'"
                cursor.execute(delete_sql)
                logger.info(f"Datos anteriores a {cutoff_date} eliminados")
        except Exception as e:
            logger.error(f"Error en cleanup: {e}")