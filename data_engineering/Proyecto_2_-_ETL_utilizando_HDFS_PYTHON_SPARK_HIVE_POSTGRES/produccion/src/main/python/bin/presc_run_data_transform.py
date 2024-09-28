from pyspark.sql.functions import upper,size, countDistinct, sum, dense_rank, col, split
from pyspark.sql.window import Window
import logging
import logging.config

# Cargar el Archivo de Configuración de Logging
logging.config.fileConfig(fname='../util/logging_to_file.conf')

# Obtener el custom Logger desde el archivo de configuracion Logging
logger = logging.getLogger(__name__)

def city_report(df_city_sel, df_fact_sel):
    """
# City Report:q21
       Transform Logics:
       1. Calculate the Number of zips in each city.
       2. Calculate the number of distinct Prescribers assigned for each City.
       3. Calculate total TRX_CNT prescribed for each city.
       4. Do not report a city in the final report if no prescriber is assigned to it.

# Informe sobre la ciudad:q21
       Lógicas de transformación:
       1. Calcular el Número de zips en cada ciudad.
       2. Calcular el número de Prescriptores distintos asignados para cada Ciudad.
       3. Calcular el total de TRX_CNT prescritos para cada ciudad.
       4. No incluir una ciudad en el informe final si no tiene asignado ningún prescriptor.       

    Layout:
       City Name
       State Name
       County Name
       City Population
       Number of Zips
       Prescriber Counts
       Total Trx counts
    """
    try:
        logger.info(f"Transform - city_report() se ha iniciado...")
        df_city_split = df_city_sel.withColumn('zip_counts',size(split(col('zips'), ' ')))
        df_fact_grp = df_fact_sel.groupBy(df_fact_sel.presc_state, df_fact_sel.presc_city).agg(countDistinct("presc_id").alias("presc_counts"), sum("trx_cnt").alias("trx_counts"))
        df_city_join = df_city_split.join(df_fact_grp,(df_city_split.state_id == df_fact_grp.presc_state) & (df_city_split.city == df_fact_grp.presc_city),'inner')
        df_city_final = df_city_join.select("city","state_name","county_name","population","zip_counts","trx_counts","presc_counts")
    except Exception as exp:
        logger.error("Error en el metodo - city_report(). Por favor chequear el Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("Transform - city_report() a finalizado...")
    return df_city_final


def top_5_Prescribers(df_fact_sel):
    """
    # Prescriber Report:
    Top 5 Prescribers with highest trx_cnt per each state.
    Consider the prescribers only from 20 to 50 years of experience.

    # Informe sobre prescriptores:
    Top 5 Prescriptores con mayor trx_cnt por cada estado.
    Considere los prescriptores sólo de 20 a 50 años de experiencia.    

    Layout:
      Prescriber ID
      Prescriber Full Name
      Prescriber State
      Prescriber Country
      Prescriber Years of Experience
      Total TRX Count
      Total Days Supply
      Total Drug Cost
    """
    try:
        logger.info("Transform - top_5_Prescribers() se ha iniciado...")
        spec = Window.partitionBy("presc_state").orderBy(col("trx_cnt").desc())
        df_presc_final = df_fact_sel.select("presc_id","presc_fullname","presc_state","country_name","year_exp","trx_cnt","total_day_supply","total_drug_cost") \
           .filter((df_fact_sel.year_exp >= 20) & (df_fact_sel.year_exp <= 50) ) \
           .withColumn("dense_rank",dense_rank().over(spec)) \
           .filter(col("dense_rank") <= 5) \
           .select("presc_id","presc_fullname","presc_state","country_name","year_exp","trx_cnt","total_day_supply","total_drug_cost")
    except Exception as exp:
        logger.error("Error en el metodo - top_5_Prescribers(). Por favor chequear el Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("Transform - top_5_Prescribers() a finalizado...")
    return df_presc_final





