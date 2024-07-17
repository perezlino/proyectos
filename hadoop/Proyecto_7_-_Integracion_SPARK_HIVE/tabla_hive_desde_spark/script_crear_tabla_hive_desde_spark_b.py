# -*- coding: utf-8 -*-    <----- Es propio de Python, nos permite usar tildes y caracteres especiales

###
 # @section Librerías
 ##

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

# Configuración de la sesión Spark
spark = SparkSession.builder \
    .appName("Ejemplo Python Spark SQL Hive Integration") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.sql.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

def main():

    # Crear un DataFrame de ejemplo
    data = [("John", 28), ("Alice", 25), ("Bob", 30)]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, schema=columns)

    # Registrar el DataFrame como una tabla temporal en Spark SQL
    df.createOrReplaceTempView("people_temp")

    ### Podriamos crear una base de datos, utilizarla y luego persistir una tabla en dicha base de datos
    # spark.sql(""" create database db_people """)
    # spark.sql(""" use db_people""") 
    # o simplemente escribir ===> df.write.saveAsTable('db.people.sampleTable')    

    # Guardar el DataFrame como una tabla gestionada en Spark SQL
    # Se guarda en la base de datos "default" que es aquella con la que se trabaja
    # por defecto si es que no se crea una
    df.write.saveAsTable("default.people_managed")

    # Consultar la tabla temporal desde Spark SQL
    spark.sql("SELECT * FROM people_temp").show()

    # Consultar la tabla gestionada desde Spark SQL
    spark.sql("SELECT * FROM default.people_managed").show()

    # Consultar la tabla gestionada desde Hive (usando beeline o CLI de Hive)
    # Ejemplo:
    # $ beeline -u jdbc:hive2://localhost:10000
    # hive> SELECT * FROM default.people_managed;

if __name__ == "__main__":
    main()

