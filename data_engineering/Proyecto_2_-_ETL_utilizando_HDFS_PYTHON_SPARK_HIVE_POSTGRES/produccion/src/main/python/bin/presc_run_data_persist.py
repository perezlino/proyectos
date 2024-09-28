import datetime as date
from pyspark.sql.functions import lit

import logging
import logging.config

# Cargar el Archivo de Configuración de Logging
logging.config.fileConfig(fname='../util/logging_to_file.conf')

# Obtener el custom Logger desde el archivo de configuracion Logging
logger = logging.getLogger(__name__)

def data_persist_hive(spark, df, dfName, partitionBy, mode):
    try:
        logger.info(f"Persistir la data del script Hive - data_persist() se inicia para guardar dataframe "+ dfName + " en la tabla Hive...")
        # Añadir una columna estatica con la fecha actual
        df=df.withColumn("delivery_date", lit(date.datetime.now().strftime("%Y-%m-%d")))
        spark.sql(""" create database if not exists prescpipeline location '/user/hive/warehouse/prescpipeline' """)
        spark.sql(""" use prescpipeline """)
        df.write.saveAsTable(dfName, partitionBy='delivery_date', mode=mode)   
    except Exception as exp:
        logger.error("Error en el metodo - data_persist_hive().Por favor, verifica el Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("Persistir data - data_persist_hive() a finalizado para guardar dataframe "+ dfName +" en la tabla Hive...")

def data_persist_postgre(spark, df, dfName, url, driver, dbtable, mode, user, password):
    try:
        logger.info(f"Persistir la data del script Postgres  - data_persist_rdbms() se inicia para guardar dataframe "+ dfName + " en la tabla Postgres...")
        df.write.format("jdbc")\
                .option("url", url) \
                .option("driver", driver) \
                .option("dbtable", dbtable) \
                .mode(mode) \
                .option("user", user) \
                .option("password", password) \
                .save()
    except Exception as exp:
        logger.error("Error en el método - data_persist_postgre(). Por favor, verifica el Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("Persistir data Postgres - data_persist_postgre() a finalizado para guardar dataframe "+ dfName +" en la tabla Postgres...")

