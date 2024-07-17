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
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

def main():
    # Carga de datos desde un archivo CSV
    df_load = spark.read.format("csv") \
        .option("inferSchema", True) \
        .option("header", True) \
        .option("sep", ",") \
        .load("/user/dept.txt")

    # Seleccionar las columnas necesarias
    df_load = df_load.select('id', 'nombre', 'salario', 'departamento')

    # Mostrar el esquema del DataFrame cargado
    df_load.printSchema()

    # Crear la base de datos si no existe
    spark.sql('CREATE DATABASE IF NOT EXISTS db1 LOCATION \'/user/databases/db1\'')

    # Crear la tabla particionada en Hive

    # Esta tabla la puedo trabajar desde Hive también
    
    # Al definir la tabla dept_partition con "STORED AS ORC" asegura que la tabla sea compatible 
    # con transacciones ACID en Hive.

    # Cuando se utiliza el formato de almacenamiento "ORC" en Hive, no se especifica "ROW FORMAT DELIMITED",
    # este es necesario solo para formatos como TEXTFILE.

    # aL Crear una tabla particionada en Hive con propiedades transaccionales ('transactional'='true') 
    # cuando la tabla es externa (EXTERNAL) no pueden ser declaradas como transaccionales.
    spark.sql("""
        CREATE TABLE IF NOT EXISTS db1.dept_partition (
            deptid INT,
            nombre STRING,
            salario INT
        )
        PARTITIONED BY (departamento STRING)
        STORED AS ORC
        LOCATION '/user/databases/db1/dept_partition'
        TBLPROPERTIES (
            'hive.exec.dynamic.partition.mode'='nonstrict',
            'hive.exec.dynamic.partition'='true',
            'orc.compress'='SNAPPY'
        )
    """)

    # Escribir datos en la tabla particionada
    df_load.write.mode('overwrite').insertInto("db1.dept_partition")

    # Mostrar algunos datos de la tabla
    spark.sql("SELECT * FROM db1.dept_partition LIMIT 10").show()

    # Mostrar las particiones de la tabla
    spark.sql("SHOW PARTITIONS db1.dept_partition").show()

    # Consultar la tabla gestionada desde Hive (usando beeline o CLI de Hive)
    # Ejemplo:
    # $ beeline -u jdbc:hive2://localhost:10000
    # hive> SELECT * FROM db1.dept_partition;

if __name__ == "__main__":
    main()
