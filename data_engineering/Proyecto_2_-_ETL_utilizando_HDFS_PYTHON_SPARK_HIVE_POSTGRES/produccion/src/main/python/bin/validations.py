import logging
import logging.config
import pandas

# Cargar el Archivo de Configuración de Logging
logging.config.fileConfig(fname='../util/logging_to_file.conf')

# Obtener el custom Logger desde el archivo de configuracion Logging
logger = logging.getLogger(__name__)

def get_curr_date(spark):
    try:
        opDF = spark.sql(""" select current_date """)
        logger.info("Validar el Spark object imprimiendo la fecha actual - " + str(opDF.collect()))
    except NameError as exp:
        logger.error("NameError en el metodo - spark_curr_date(). Por favor chequear el Stack Trace. " + str(exp),exc_info=True)
        # Estamos elevando (raising, que proviene de "raise") este error para que pueda volver al script padre, que es "run_presc_pipeline.py"
        # Captura y Propagación: Al usar "raise" dentro de un bloque except, es importante tener en cuenta que si no se maneja la excepción
        # localmente, se propagará hacia arriba en la pila de llamadas hasta que se encuentre un manejo adecuado o hasta que el programa finalice
        # si no se maneja en absoluto. En nuestro caso, escala hasta el script padre que vendria a ser "run_presc_pipeline.py".
        raise
    except Exception as exp:
        logger.error("Error en el metodo - spark_curr_date(). Por favor chequear el Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("El Spark object a sido validado. El Spark Object esta listo !!! \n\n")

def df_count(df,dfName):
    try:
        logger.info(f"La validación del dataframe por recuento df_count() se a iniciado para el Dataframe {dfName}...")
        df_count=df.count()
        logger.info(f"El recuento del DataFrame es {df_count}.")
    except Exception as exp:
        logger.error("Error en el metodo - df_count(). Por favor chequear el Stack Trace. " + str(exp))
        raise
    else:
        logger.info(f"La validación del dataframe por recuento df_count() a finalizado !!! \n\n")

def df_top10_rec(df,dfName):
    try:
        logger.info(f"La validación del dataframe por los 10 primeros registros df_top10_rec() se a iniciado para el Dataframe {dfName}...")
        logger.info(f"Los 10 primeros registros del DataFrame son:")
        # Desde Spark es muy difícil imprimir los registros en el logger porque el logger está mucho
        # más conectado a Python que PySpark. Así que podemos hacer es convertir este Spark dataframe
        # en un Pandas dataframe.
        df_pandas=df.limit(10).toPandas()

        # con "to_string()" convertimos el dataframe pandas a string y "index=False" le quitamos el indice
        logger.info('\n \t'+ df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error en el metodo - df_top10_rec(). Por favor chequear el Stack Trace. " + str(exp))
        raise
    else:
        logger.info("La validación del dataframe por los 10 primeros registros df_top10_rec() a finalizado !!! \n\n")

def df_print_schema(df,dfName):
    try:
        logger.info(f"La validación del Schema DataFrame para el dataframe {dfName}...")

        # No utilizamos "df.printSschema()" dado que el resultado que devuelve este metodo es del tipo "NoneType"
        # y da problemas al utilizarlo en el logger
        sch=df.schema.fields
        logger.info(f"El schema del DataFrame es {dfName}: ")

        # Hacemos un for loop el cual irá imprimiendo el schema de cada campo
        for i in sch:
            logger.info(f"\t{i}")
    except Exception as exp:
        logger.error("Error en el metodo - df_show_schema(). Por favor chequear el Stack Trace. " + str(exp))
        raise
    else:
        logger.info("La validacion del Schema Dataframe a finalizado !!! \n\n")










