# -*- coding: utf-8 -*-    <----- Es propio de Python, nos permite usar tildes y caracteres especiales

###
 # @section Librerías
 ##

import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql import SparkSession

###
 # @section Tuning
 ##

spark = SparkSession.builder.\
appName("Ejemplo Python Spark SQL Hive Integration").\
config("spark.sql.uris", "thrift://hive-metastore:9083").\
config("spark.master", "spark://spark-master:7077").\
enableHiveSupport().\
getOrCreate()

###
 # @section Programa
 ##

# Carga de datos desde un archivo CSV
df_load = spark.read.format("csv") \
    .option("inferSchema", True) \
    .option("header", True) \
    .option("sep", ",") \
    .load("/user/movies.csv")

df_load.show(truncate=False)

spark.sql('DROP TABLE IF EXISTS movies')
spark.sql('DROP TABLE IF EXISTS movies_orc')
spark.sql('DROP TABLE IF EXISTS movies_avro')
spark.sql('DROP TABLE IF EXISTS movies_parquet')

spark.sql("CREATE TABLE movies ( \
          movieid INT, \
          title STRING, \
          genre STRING \
          ) STORED AS TEXTFILE")

spark.sql("CREATE TABLE movies_orc ( \
          movieid INT, \
          title STRING, \
          genre STRING \
          ) STORED AS ORC")

spark.sql("CREATE TABLE movies_avro ( \
          movieid INT, \
          title STRING, \
          genre STRING \
          ) STORED AS AVRO")

spark.sql("CREATE TABLE movies_parquet ( \
          movieid INT, \
          title STRING, \
          genre STRING \
          ) STORED AS PARQUET")

df_load.write.mode('overwrite').insertInto('movies')
df_load.write.mode('overwrite').insertInto('movies_orc')
df_load.write.mode('overwrite').insertInto('movies_avro')
df_load.write.mode('overwrite').insertInto('movies_parquet')