import logging
import logging.config
from pyspark.sql.functions import upper, lit, regexp_extract, col, concat_ws, count, isnan, when,  avg, round, coalesce
from pyspark.sql.window import Window

# Cargar el Archivo de Configuración de Logging
logging.config.fileConfig(fname='../util/logging_to_file.conf')

# Obtener el custom Logger desde el archivo de configuracion Logging
logger = logging.getLogger(__name__)

def perform_data_clean(df1,df2):
    ### Limpieza del dataframe "df_city":
    #1 Seleccionar sólo las columnas necesarias: 'city', 'state_id', 'state_name', 'county_name', 'population' y 'zip'
    #2 Convertir los campos de 'city', 'state' y 'county' a mayúsculas y renombrarlos
    try:
        logger.info(f" La funcion perform_data_clean() se ha iniciado para el dataframe df_city...")
        df_city_sel = df1.select(upper(df1.city).alias("city"),
                                 df1.state_id,
                                 upper(df1.state_name).alias("state_name"),
                                 upper(df1.county_name).alias("county_name"),
                                 df1.population,
                                 df1.zips)

    ### Limpieza del dataframe "df_fact":
    #1 Seleccionar sólo las columnas necesarias: 'npi', 'nppes_provider_last_org_name', 'nppes_provider_first_name', 'nppes_provider_city'
    #                                            'nppes_provider_state', 'specialty_description', 'year_exp', 'drug_name', 'total_claim_count',
    #                                            'total_day_supply' y 'total_drug_cost'
    #2 Renombrar las columnas 
        logger.info(f"perform_data_clean() is started for df_fact dataframe...")
        df_fact_sel = df2.select(df2.npi.alias("presc_id"),df2.nppes_provider_last_org_name.alias("presc_lname"), \
                             df2.nppes_provider_first_name.alias("presc_fname"),df2.nppes_provider_city.alias("presc_city"), \
                             df2.nppes_provider_state.alias("presc_state"),df2.specialty_description.alias("presc_spclt"), df2.year_exp, \
                             df2.drug_name,df2.total_claim_count.alias("trx_cnt"),df2.total_day_supply, \
                             df2.total_drug_cost)
    
    #3 Añadir un campo de país llamado 'country_name' con el valor estático 'USA'
        df_fact_sel = df_fact_sel.withColumn("country_name",lit("USA"))

    #4 Limpieza del campo 'year_exp'
        pattern = r'\d+'
        idx = 0
        df_fact_sel = df_fact_sel.withColumn("year_exp",regexp_extract(col("year_exp"),pattern,idx))

    #5 Convertir el tipo de datos year_exp de string a entero
        df_fact_sel = df_fact_sel.withColumn("year_exp",col("year_exp").cast("int"))

    #6 Concatenar el campo 'presc_fname' y 'presc_lname', para formar el campo 'presc_fullname'
        df_fact_sel = df_fact_sel.withColumn("presc_fullname",concat_ws(" ", "presc_fname", "presc_lname"))
        df_fact_sel = df_fact_sel.drop("presc_fname", "presc_lname")

    #7 Comprobar y limpiar todos los valores Null/Nan
        # Contabilización de "Nulls" y "NaN" utilizando bucle `for`
        # El resultado saldrá en la consola
        #df_fact_sel.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_fact_sel.columns]).show()

    #8 Eliminar los registros donde PRESC_ID sea NULL
        df_fact_sel = df_fact_sel.dropna(subset="presc_id")

    #9 Eliminar los registros en los que DRUG_NAME es NULL
        df_fact_sel = df_fact_sel.dropna(subset="drug_name")

    #10 Asignar TRX_CNT cuando es nulo como media de trx_cnt para ese prescriptor
        # Utilizando ventanas se calcula el promedio por ventana, y cada ventana corresponde
        # a las agrupaciones de la columna "presc_id". Es decir, una ventana para todos los valores "id"
        # iguales de la columna "presc_id". Se utilizó la función "coalesce()" para rellenar este promedio donde existen
        # nulos en la columna "trx_cnt".
        spec = Window.partitionBy("presc_id")
        df_fact_sel = df_fact_sel.withColumn('trx_cnt', coalesce("trx_cnt",round(avg("trx_cnt").over(spec))))
        df_fact_sel=df_fact_sel.withColumn("trx_cnt",col("trx_cnt").cast('integer'))

        # Check and clean all the Null/Nan Values
        #df_fact_sel.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_fact_sel.columns]).show()


    except Exception as exp:
        logger.error("Error en el metodo - spark_curr_date(). Por favor, verifica el Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("perform_data_clean() a finalizado...")
    return df_city_sel,df_fact_sel