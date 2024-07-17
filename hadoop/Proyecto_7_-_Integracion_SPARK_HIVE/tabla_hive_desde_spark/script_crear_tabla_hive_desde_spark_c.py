# -*- coding: utf-8 -*-    <----- Es propio de Python, nos permite usar tildes y caracteres especiales

###
 # @section Librerías
 ##

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as f
import datetime as date

# Configuración de la sesión Spark
spark = SparkSession.builder \
    .appName("Ejemplo Python Spark SQL Hive Integration") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.sql.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

def main():

    # Crear una Base de datos en Hive si no existe
    spark.sql("CREATE DATABASE IF NOT EXISTS prescpipeline LOCATION '/user/bigdata/hive/prescpipeline.db'")

    # Usar la base de datos recién creada
    spark.sql("USE prescpipeline")

    # Definir el esquema del DataFrame
    schema = "EmpName STRING, EmpAge INT"

    # Especificar la ubicación en HDFS para la tabla
    sample_location = "/user/hive/warehouse/prescpipeline.db/sampleTable"

    # Crear la tabla en Hive con LOCATION especificado en HDFS
    spark.sql(f"CREATE TABLE IF NOT EXISTS sampleTable ({schema}) LOCATION '{sample_location}'")

    # Definir el esquema del DataFrame
    schema = StructType([
        StructField("EmpName", StringType(), True),
        StructField("EmpAge", IntegerType(), True)
    ])

    # Crear el DataFrame con datos y esquema definido
    data = [('Robert', 25), ('Reid', 35), ('Ram', 21)]
    df = spark.createDataFrame(data, schema)
    df.show()
    df.printSchema()

    # Escribir el DataFrame en una tabla Hive llamada sampleTable
    df.write.mode('overwrite').insertInto('prescpipeline.sampleTable')

    # Consultar y mostrar el contenido de la tabla
    spark.sql("SELECT * FROM prescpipeline.sampleTable").show()

    ### Comprobar el archivo subyacente de tablas Hive en HDFS
    # hdfs dfs -ls /hive/warehouse/prescpipeline.db/sampletable  

    # Consultar la tabla gestionada desde Hive (usando beeline o CLI de Hive)
    # Ejemplo:
    # $ beeline -u jdbc:hive2://localhost:10000
    # hive> SELECT * FROM prescpipeline.sampleTable;    
    # hive> DESCRIBE FORMATTED prescpipeline.sampleTable;      


if __name__ == "__main__":
    main()

