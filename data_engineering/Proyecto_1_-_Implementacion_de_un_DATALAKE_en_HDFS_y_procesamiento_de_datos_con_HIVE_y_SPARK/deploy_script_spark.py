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
appName("Cargar datos a la tabla TRANSACCION_ENRIQUECIDA").\
config("spark.master", "spark://spark-master:7077").\
enableHiveSupport().\
getOrCreate()

### Recordar que por defecto en Spark al crear una tabla en Spark, el formato de archivo 
### predeterminado para el almacenamiento de datos es "Parquet". Y si no especificas un 
### formato de compresión, la compresión predeterminada para los archivos Parquet será "Snappy".

###
 # @section Programa
 ##

def main():

	###
	 # @section Lectura de datos
	 ##
	#
	dfPersona = spark.sql(f"""
		SELECT 
			* 
		FROM 
			UNIVERSAL.PERSONA
	"""
	)
	#
	dfPersona.show()
	#
	dfEmpresa = spark.sql(f"""
		SELECT 
			* 
		FROM 
			UNIVERSAL.EMPRESA
	"""
	)
	#
	dfEmpresa.show()
	#
	dfTransaccion = spark.sql(f"""
		SELECT 
			* 
		FROM 
			UNIVERSAL.TRANSACCION
	"""
	)
	#
	dfTransaccion.show()
	#
	###
	 # @section Procesamiento
	###
	#
	#Obtenemos el nombre de la empresa donde trabaja la persona
	dfPersonaEnriquecida = dfPersona.alias("P").join(
		dfEmpresa.alias("E"), 
		f.col("P.ID_EMPRESA") == f.col("E.ID")
	).select(
		f.col("P.ID").alias("ID_PERSONA"),
		f.col("P.NOMBRE").alias("NOMBRE_PERSONA"),
		f.col("P.EDAD").alias("EDAD_PERSONA"),
		f.col("P.SALARIO").alias("SALARIO_PERSONA"),
		f.col("E.NOMBRE").alias("TRABAJO_PERSONA")
	)
	#
	dfPersonaEnriquecida.show()
	#
	#Enriquecemos la transacción con los datos de la persona
	dfPersonaEnriquecidaTransaccion = dfPersonaEnriquecida.alias("P").join(
		dfTransaccion.alias("T"), 
		f.col("P.ID_PERSONA") == f.col("T.ID_PERSONA")
	).select(
		f.col("P.ID_PERSONA").alias("ID_PERSONA"),
		f.col("P.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
		f.col("P.EDAD_PERSONA").alias("EDAD_PERSONA"),
		f.col("P.SALARIO_PERSONA").alias("SALARIO_PERSONA"),
		f.col("P.TRABAJO_PERSONA").alias("TRABAJO_PERSONA"),
		f.col("T.ID_EMPRESA").alias("ID_EMPRESA_TRANSACCION"),
		f.col("T.MONTO").alias("MONTO_TRANSACCION"),
		f.col("T.FECHA").alias("FECHA_TRANSACCION")
	)
	#
	dfPersonaEnriquecidaTransaccion.show()
	#
	#Enriquecemos la transacción colocando el nombre de la empresa donde se realizó la transacción
	dfTransaccionEnriquecida = dfPersonaEnriquecidaTransaccion.alias("P").join(
		dfEmpresa.alias("E"), 
		f.col("P.ID_EMPRESA_TRANSACCION") == f.col("E.ID")
	).select(
		f.col("P.ID_PERSONA").alias("ID_PERSONA"),
		f.col("P.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
		f.col("P.EDAD_PERSONA").alias("EDAD_PERSONA"),
		f.col("P.SALARIO_PERSONA").alias("SALARIO_PERSONA"),
		f.col("P.TRABAJO_PERSONA").alias("TRABAJO_PERSONA"),
		f.col("P.MONTO_TRANSACCION").alias("MONTO_TRANSACCION"),
		f.col("P.FECHA_TRANSACCION").alias("FECHA_TRANSACCION"),
		f.col("E.NOMBRE").alias("EMPRESA_TRANSACCION")
	)
	#
	dfTransaccionEnriquecida.show()
	#
	#Guardamos el dataframe de reporte en la tabla Hive
	dfTransaccionEnriquecida.createOrReplaceTempView("dfReporte")
	#
	#Activamos el particionamiento dinamico. 
	#Es necesario utilizar ambas propiedades si planeas realizar particiones dinámicas en Hive a través de Spark. 	
	
	spark.sql("SET hive.exec.dynamic.partition=true") 
	spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
	#
	#

	#Truncar tabla UNIVERSAL.TRANSACCION_ENRIQUECIDA
	spark.sql("TRUNCATE TABLE UNIVERSAL.TRANSACCION_ENRIQUECIDA")
	#
	#Almacenamos el resultado en Hive
	spark.sql(f"""
		INSERT INTO UNIVERSAL.TRANSACCION_ENRIQUECIDA
			SELECT
				*
			FROM 
				dfReporte T
	"""
	)
	#
	#Verificamos que el reporte se haya insertado correctamente
	spark.sql(f"""
		SELECT 
			* 
		FROM 
			UNIVERSAL.TRANSACCION_ENRIQUECIDA 
		LIMIT 10
	"""
	).show()

main()