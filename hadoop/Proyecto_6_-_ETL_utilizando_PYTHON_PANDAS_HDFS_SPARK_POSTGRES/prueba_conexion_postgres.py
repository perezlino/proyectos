# spark-submit --master spark://spark-master:7077 --jars /proyectos/PrescPipeline/src/main/python/lib/postgresql-42.2.23.jar prueba_conexion_postgres.py

from pyspark.sql import SparkSession

# Configuración de Spark
spark = SparkSession.builder \
    .appName("Persistir en PostgreSQL") \
    .config("spark.driver.extraClassPath", "/lib/postgresql-42.2.23.jar") \
    .getOrCreate()


# DataFrame de ejemplo
data = [("Alice", 1), ("Bob", 2)]
columns = ["name", "ae"]
df = spark.createDataFrame(data, schema=columns)

# Opciones de conexión a PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Guardar el DataFrame en PostgreSQL
df.write.jdbc(url=jdbc_url, table="prueba", mode="overwrite", properties=properties)

# Cerrar la sesión de Spark
spark.stop()