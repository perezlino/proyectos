### Importar todos los modulos necesarios
import get_all_variables as gav
from create_objects import get_spark_object
from create_db import create_database
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
import sys
import logging
import logging.config
import os
from presc_run_data_ingest import load_files
from presc_run_data_preprocessing import perform_data_clean
from presc_run_data_transform import city_report, top_5_Prescribers
from subprocess import Popen, PIPE
from presc_run_data_extraction import extract_files
from presc_run_data_persist import data_persist_hive, data_persist_postgre

# Cargar el Archivo de Configuración de Logging
logging.config.fileConfig(fname='../util/logging_to_file.conf')

def main():
   # Llamar a la función para crear la base de datos antes de continuar
    create_database('prescpipeline')
    try:
        logging.info("main() se ha iniciado ...")
        ### Obtener el Spark Object
        spark = get_spark_object(gav.environ,gav.appName)
        # Validar el Spark Object
        get_curr_date(spark)

        ### Iniciar el script "presc_run_data_ingest" 
        # Cargar el archivo del directorio "dimension_city"
        file_dir="/proyectos/PrescPipeline/staging/dimension_city"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
           file_format = 'parquet'
           header='NA'
           inferSchema='NA'
        elif 'csv' in out.decode():
           file_format = 'csv'
           header=gav.header
           inferSchema=gav.inferSchema

        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format , header=header, inferSchema=inferSchema)

        # Cargar el archivo Prescriber que se encuentra en el directorio "fact"
        file_dir="/proyectos/PrescPipeline/staging/fact"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
           file_format = 'parquet'
           header='NA'
           inferSchema='NA'
        elif 'csv' in out.decode():
           file_format = 'csv'
           header=gav.header
           inferSchema=gav.inferSchema

        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format , header=header, inferSchema=inferSchema)

        ### Validar el script "run_data_ingest" para "df_city" & "df_fact"
        df_count(df_city,'df_city')
        df_top10_rec(df_city,'df_city')

        df_count(df_fact,'df_fact')
        df_top10_rec(df_fact,'df_fact')

        ### Iniciar el script "presc_run_data_preprocessing"
        ## Realizar operaciones de limpieza de datos para "df_city" y "df_fact"
        df_city_sel,df_fact_sel = perform_data_clean(df_city,df_fact)

        # Validacion para los dataframes "df_city" y "df_fact"
        df_top10_rec(df_city_sel,'df_city_sel')
        df_top10_rec(df_fact_sel,'df_fact_sel')
        df_print_schema(df_fact_sel,'df_fact_sel')

        ### Iniciar el script "presc_run_data_transform"
        df_city_final = city_report(df_city_sel,df_fact_sel)
        df_presc_final = top_5_Prescribers(df_fact_sel)

        # Validacion para el dataframe "df_city_final" y "df_presc_final"
        df_top10_rec(df_city_final,'df_city_final')
        df_print_schema(df_city_final,'df_city_final')
        df_count(df_city_final,'df_city_final')
        df_top10_rec(df_presc_final,'df_presc_final')
        df_print_schema(df_presc_final,'df_presc_final')
        df_count(df_presc_final,'df_presc_final')

        ### Iniciar el script "run_data_extraction"
        CITY_PATH=gav.output_city
        extract_files(df_city_final,'json',CITY_PATH,1,False,'bzip2')
     
        PRESC_PATH=gav.output_fact
        extract_files(df_presc_final,'orc',PRESC_PATH,2,False,'snappy')

        ### Persistir data
        # Persistir data en Hive
        data_persist_hive(spark=spark, df=df_city_final,  dfName='df_city_final',  partitionBy='delivery_date', mode='append')        
        data_persist_hive(spark=spark, df=df_presc_final, dfName='df_presc_final', partitionBy='delivery_date', mode='append')

        # Persistir data en Postgres
        data_persist_postgre(spark=spark, df=df_city_final,  dfName='df_city_final',  url="jdbc:postgresql://postgres:5432/prescpipeline", driver="org.postgresql.Driver", dbtable='df_city_final', mode="append", user=gav.user, password=gav.password)
        data_persist_postgre(spark=spark, df=df_presc_final, dfName='df_presc_final', url="jdbc:postgresql://postgres:5432/prescpipeline", driver="org.postgresql.Driver", dbtable='df_presc_final', mode="append", user=gav.user, password=gav.password)

        ### Fin de la aplicacion
        logging.info("run_presc_pipeline.py a finalizado.")

    except Exception as exp:
        logging.error("Error en el metodo main(). Por favor chequear el Stack Trace para ir al respectivo modulo"
              " y arreglarlo." +str(exp),exc_info=True)
        sys.exit(1)

if __name__ == "__main__" :
    logging.info("run_presc_pipeline se ha iniciado ...")
    main()