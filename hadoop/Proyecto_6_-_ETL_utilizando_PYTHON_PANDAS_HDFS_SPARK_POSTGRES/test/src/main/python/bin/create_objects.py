from pyspark.sql import SparkSession
import logging
import logging.config

# Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

def get_spark_object(envn,appName ):
    try:
        logger.info(f"get_spark_object() is started. The '{envn}' envn is used.")
        if envn == 'TEST' :
            master='local'
        else:
            master='yarn'
        spark = SparkSession \
                  .builder \
                  .master(master) \
                  .appName(appName) \
                  .getOrCreate()
    except NameError as exp:
        logger.error("NameError in the method - get_spark_object(). Please check the Stack Trace. " + str(exp),exc_info=True)
        # Estamos elevando (raising, que proviene de "raise") este error para que pueda volver al script padre, que es "run_presc_pipeline.py"
        # Captura y Propagación: Al usar "raise" dentro de un bloque except, es importante tener en cuenta que si no se maneja la excepción
        # localmente, se propagará hacia arriba en la pila de llamadas hasta que se encuentre un manejo adecuado o hasta que el programa finalice
        # si no se maneja en absoluto. En nuestro caso, escala hasta el script padre que vendria a ser "run_presc_pipeline.py".
        raise
    except Exception as exp:
        logger.error("Error in the method - get_spark_object(). Please check the Stack Trace. " + str(exp),exc_info=True)
    else:
        logger.info("Spark Object is created ...")
    return spark



