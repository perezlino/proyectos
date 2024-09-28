import logging
import logging.config

# Cargar el Archivo de Configuración de Logging
logging.config.fileConfig(fname='../util/logging_to_file.conf')

# Obtener el custom Logger desde el archivo de configuracion Logging
logger = logging.getLogger(__name__)

def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.info("load_files() ha comenzado ...")
        if file_format == 'parquet' :
            df = spark. \
                read. \
                format(file_format). \
                load(file_dir)
        elif file_format == 'csv' :
            df = spark. \
                read. \
                format(file_format). \
                options(header=header). \
                options(inferSchema=inferSchema). \
                load(file_dir)
    except Exception as exp:
        logger.error("Error en el método - load_files(). Por favor, verifica el Stack Trace. " + str(exp))
        # Estamos elevando (raising, que proviene de "raise") este error para que pueda volver al script padre, que es "run_presc_pipeline.py"
        # Captura y Propagación: Al usar "raise" dentro de un bloque except, es importante tener en cuenta que si no se maneja la excepción
        # localmente, se propagará hacia arriba en la pila de llamadas hasta que se encuentre un manejo adecuado o hasta que el programa finalice
        # si no se maneja en absoluto. En nuestro caso, escala hasta el script padre que vendria a ser "run_presc_pipeline.py".
        raise
    else:
        logger.info(f"El archivo input {file_dir} a sido cargado en el dataframe. La funcion load_files() a finalizado.")
    return df