"""
Script principal para ejecutar el sistema ETL.
"""

import sys
from pathlib import Path
import yaml
import argparse
from loguru import logger

# Añadir path del proyecto
sys.path.append(str(Path(__file__).parent.parent))

from etl_system.extractor import DataExtractor
from etl_system.transformer import DataTransformer
from etl_system.loader import HiveLoader
from etl_system.batch_manager import BatchManager, BatchScheduler


def setup_logging(log_level: str = "INFO"):
    """Configura el sistema de logging."""
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        level=log_level,
    )
    logger.add(
        "data/logs/etl_{time}.log",
        rotation="500 MB",
        retention="10 days",
        level="DEBUG",
    )


def load_config(config_path: str = "config/etl_config.yaml") -> dict:
    """Carga configuración desde YAML."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


def run_etl_pipeline(
    source_type: str,
    config: dict,
    extractor: DataExtractor,
    transformer: DataTransformer,
    loader: HiveLoader,
    batch_manager: BatchManager,
):
    """
    Ejecuta el pipeline ETL completo para un tipo de fuente.

    Args:
        source_type: 'video' o 'image'
        config: Configuración
        extractor: Extractor de datos
        transformer: Transformador de datos
        loader: Loader de Hive
        batch_manager: Gestor de lotes
    """
    logger.info(f"=== Iniciando ETL para {source_type} ===")

    try:
        # 1. EXTRACCIÓN
        logger.info("Fase 1: Extracción")

        # Obtener archivos pendientes
        all_files = extractor.scan_staging_files()
        pending_files = batch_manager.get_pending_files(
            [f.name for f in all_files[source_type]], source_type=source_type
        )

        if not pending_files:
            logger.info(f"No hay archivos pendientes de {source_type}")
            return

        # Extraer datos
        files_to_process = [
            f for f in all_files[source_type] if f.name in pending_files
        ]
        df_raw = extractor.extract_multiple(files_to_process)

        if df_raw.empty:
            logger.warning("No se extrajeron datos")
            return

        logger.info(f"Extraídas {len(df_raw)} filas de {len(files_to_process)} archivos")

        # 2. TRANSFORMACIÓN
        logger.info("Fase 2: Transformación")

        df_transformed = transformer.transform_pipeline(df_raw, add_features=True)

        # Obtener estadísticas de transformación
        stats = transformer.get_transformation_stats(df_raw, df_transformed)
        logger.info(f"Transformación: {stats['rows_after']} filas válidas")
        logger.info(f"Filas removidas: {stats['rows_removed']} ({stats['removal_percentage']}%)")

        # 3. CARGA
        logger.info("Fase 3: Carga a Hive")

        # Cargar con deduplicación
        loaded_count = loader.load_with_deduplication(
            df_transformed, table_name=config.get("table_name", "detections")
        )

        logger.info(f"Cargadas {loaded_count} filas nuevas a Hive")

        # 4. ACTUALIZAR ESTADO
        batch_manager.mark_files_processed(pending_files)
        batch_manager.update_statistics(loaded_count)

        if source_type == "video":
            batch_manager.update_video_batch_time()
        elif source_type == "image":
            batch_manager.update_image_batch_processed(len(pending_files))

        # Obtener estadísticas finales
        hive_stats = loader.get_table_stats()
        logger.info(f"Total de registros en Hive: {hive_stats.get('total_rows', 0)}")

        logger.info(f"=== ETL para {source_type} completado exitosamente ===")

    except Exception as e:
        logger.exception(f"Error en pipeline ETL: {e}")
        raise


def main():
    """Función principal."""
    parser = argparse.ArgumentParser(description="Sistema ETL para Detecciones")
    parser.add_argument(
        "--config",
        type=str,
        default="config/etl_config.yaml",
        help="Ruta al archivo de configuración",
    )
    parser.add_argument(
        "--source-type",
        type=str,
        choices=["video", "image", "all"],
        default="all",
        help="Tipo de fuente a procesar",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Ejecutar en modo daemon (continuo)",
    )
    parser.add_argument(
        "--init-hive",
        action="store_true",
        help="Inicializar base de datos y tablas en Hive",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Resetear estado del batch manager",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Mostrar estadísticas y salir",
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

    logger.info("=== Iniciando Sistema ETL ===")

    # Cargar configuración
    try:
        config = load_config(args.config)
        logger.info(f"Configuración cargada desde {args.config}")
    except Exception as e:
        logger.error(f"Error cargando configuración: {e}")
        sys.exit(1)

    # Inicializar componentes
    try:
        extractor = DataExtractor(
            staging_dir=config.get("staging_dir", "data/staging")
        )

        transformer = DataTransformer(
            min_confidence=config.get("min_confidence", 0.25),
            remove_duplicates=config.get("remove_duplicates", True),
        )

        hive_config = config.get("hive", {})
        loader = HiveLoader(
            host=hive_config.get("host", "localhost"),
            port=hive_config.get("port", 10000),
            database=hive_config.get("database", "detections_db"),
            username=hive_config.get("username", "hive"),
        )

        batch_manager = BatchManager(
            video_time_window=config.get("video_time_window", 300),
            image_batch_size=config.get("image_batch_size", 100),
            state_file=config.get("state_file", "data/etl_state.json"),
        )

    except Exception as e:
        logger.error(f"Error inicializando componentes: {e}")
        sys.exit(1)

    # Inicializar Hive si se solicita
    if args.init_hive:
        logger.info("Inicializando base de datos y tablas en Hive")
        try:
            loader.create_database()
            loader.create_detections_table(
                table_name=config.get("table_name", "detections")
            )
            logger.info("Hive inicializado correctamente")
        except Exception as e:
            logger.error(f"Error inicializando Hive: {e}")
            sys.exit(1)

    # Resetear estado si se solicita
    if args.reset_state:
        logger.warning("Reseteando estado del batch manager")
        batch_manager.reset_state()
        logger.info("Estado reseteado")

    # Mostrar estadísticas si se solicita
    if args.stats:
        logger.info("=== Estadísticas del Sistema ===")

        # Estadísticas del staging
        staging_summary = extractor.get_staging_summary()
        logger.info(f"Archivos en staging: {staging_summary['total_files']}")
        logger.info(f"  - Videos: {staging_summary['by_type']['video']}")
        logger.info(f"  - Imágenes: {staging_summary['by_type']['image']}")

        # Estadísticas del batch manager
        batch_stats = batch_manager.get_statistics()
        logger.info(f"Lotes procesados totales: {batch_stats['total_batches']}")
        logger.info(f"Registros procesados totales: {batch_stats['total_records']}")
        logger.info(f"Archivos procesados: {batch_stats['processed_files_count']}")

        # Estadísticas de Hive
        try:
            hive_stats = loader.get_table_stats()
            logger.info(f"Registros en Hive: {hive_stats.get('total_rows', 0)}")
            logger.info(f"Por tipo: {hive_stats.get('by_source_type', {})}")
        except Exception as e:
            logger.warning(f"No se pudieron obtener estadísticas de Hive: {e}")

        sys.exit(0)

    # Ejecutar ETL
    try:
        if args.daemon:
            # Modo daemon: ejecución continua
            logger.info("Ejecutando en modo daemon")

            def etl_callback(source_type: str):
                run_etl_pipeline(
                    source_type=source_type,
                    config=config,
                    extractor=extractor,
                    transformer=transformer,
                    loader=loader,
                    batch_manager=batch_manager,
                )

            scheduler = BatchScheduler(
                batch_manager=batch_manager,
                check_interval=config.get("check_interval", 60),
            )

            scheduler.run(etl_callback)

        else:
            # Modo único: una ejecución
            if args.source_type == "all":
                run_etl_pipeline("video", config, extractor, transformer, loader, batch_manager)
                run_etl_pipeline("image", config, extractor, transformer, loader, batch_manager)
            else:
                run_etl_pipeline(
                    args.source_type, config, extractor, transformer, loader, batch_manager
                )

    except KeyboardInterrupt:
        logger.info("ETL interrumpido por usuario")
    except Exception as e:
        logger.exception(f"Error durante ejecución del ETL: {e}")
        sys.exit(1)

    logger.info("=== Sistema ETL Finalizado ===")


if __name__ == "__main__":
    main()