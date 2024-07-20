import logging
import logging.config
import pandas

# Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

def get_curr_date(spark):
    try:
        opDF = spark.sql(""" select current_date """)
        logger.info("Validate the Spark object by printing Current Date - " + str(opDF.collect()))
    except NameError as exp:
        logger.error("NameError in the method - spark_curr_date(). Please check the Stack Trace. " + str(exp),exc_info=True)
        # Estamos elevando (raising, que proviene de "raise") este error para que pueda volver al script padre, que es "run_presc_pipeline.py"
        # Captura y Propagación: Al usar "raise" dentro de un bloque except, es importante tener en cuenta que si no se maneja la excepción
        # localmente, se propagará hacia arriba en la pila de llamadas hasta que se encuentre un manejo adecuado o hasta que el programa finalice
        # si no se maneja en absoluto. En nuestro caso, escala hasta el script padre que vendria a ser "run_presc_pipeline.py".
        raise
    except Exception as exp:
        logger.error("Error in the method - spark_curr_date(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("Spark object is validated. Spark Object is ready.")

def df_count(df,dfName):
    try:
        logger.info(f"The DataFrame Validation by count df_count() is started for Dataframe {dfName}...")
        df_count=df.count()
        logger.info(f"The DataFrame count is {df_count}.")
    except Exception as exp:
        logger.error("Error in the method - df_count(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logger.info(f"The DataFrame Validation by count df_count() is completed.")

def df_top10_rec(df,dfName):
    try:
        logger.info(f"The DataFrame Validation by top 10 record df_top10_rec() is started for Dataframe {dfName}...")
        logger.info(f"The DataFrame top 10 records are:.")
        # Desde Spark es muy difícil imprimir los registros en el logger porque el logger está mucho
        # más conectado a Python que PySpark. Así que podemos hacer es convertir este Spark dataframe
        # en un Pandas dataframe.
        df_pandas=df.limit(10).toPandas()

        # con "to_string()" convertimos el dataframe pandas a string y "index=False" le quitamos el indice
        logger.info('\n \t'+ df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error in the method - df_top10_rec(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logger.info("The DataFrame Validation by top 10 record df_top10_rec() is completed.")

def df_print_schema(df,dfName):
    try:
        logger.info(f"The DataFrame Schema Validation for Dataframe {dfName}...")

        # No utilizamos "df.printSschema()" dado que el resultado que devuelve este metodo es del tipo "NoneType"
        # y da problemas al utilizarlo en el logger
        sch=df.schema.fields
        logger.info(f"The DataFrame {dfName} schema is: ")

        # Hacemos un for loop el cual irá imprimiendo el schema de cada campo
        for i in sch:
            logger.info(f"\t{i}")
    except Exception as exp:
        logger.error("Error in the method - df_show_schema(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logger.info("The DataFrame Schema Validation is completed.")










