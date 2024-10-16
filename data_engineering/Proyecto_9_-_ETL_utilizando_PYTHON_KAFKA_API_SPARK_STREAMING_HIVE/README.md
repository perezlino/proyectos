# Proyecto de Procesamiento de Datos en tiempo real con Kafka, Spark Streaming, HDFS y Hive

[![p485.png](https://i.postimg.cc/bJ5PLQ2j/p485.png)](https://postimg.cc/MM1gpfDs)

Este proyecto esta diseñado para demostrar como integrar Apache Kafka y Apache Spark para el procesamiento de datos en tiempo real. A través de tres scripts, el proyecto permite la generación de datos, su ingesta desde Kafka y la transformación de estos datos para su almacenamiento en Hive.

## Archivos del Proyecto

1. **fake_data.py**: 
   - Este script genera mensajes a partir de un archivo CSV (`fitness_trackers.csv`) y los envía a un topico de Kafka.
   - Se configura un productor de Kafka para serializar las claves y valores en formato JSON.
   - Implementa un callback que permite rastrear el estado de entrega de cada mensaje, notificando sobre el éxito o fallo en la entrega.

2. **ingest.py**: 
   - Este script se encarga de leer los datos transmitidos desde un topico de Kafka en tiempo real.
   - Se crea un Streaming DataFrame en Spark que consume mensajes del topico de Kafka especificado.
   - Utiliza la función `process_row` para escribir los datos en formato Parquet en un sistema de archivos distribuido (HDFS), almacenándolos en un directorio de staging.

3. **transform.py**: 
   - Este script transforma los datos almacenados en el directorio de staging, aplicando un schema definido y cargándolos en una tabla de Hive.
   - Al utilizar Apache Spark con soporte para Hive, el script extrae los datos, aplica la transformación necesaria y almacena los resultados en la base de datos Hive, lo que permite un análisis posterior a través de consultas SQL.

## ¿Qué es Hive?

Apache Hive es un sistema de almacenamiento de datos diseñado para facilitar el procesamiento de grandes volúmenes de información en entornos de big data. Permite realizar consultas sobre los datos almacenados utilizando un lenguaje similar a SQL, lo que facilita el análisis y la exploración de datos.

## Tecnologias utilizadas

- **Apache Kafka**: Para la mensajería y la producción de datos.
- **Apache Spark**: Para el procesamiento y la transformación de datos.
- **Apache Hive**: Para el almacenamiento y consulta de los datos procesados.
- **Python**: Lenguaje utilizado para implementar los scripts.
- **Librerías**: `kafka-python`, `pyspark`, `json`, `csv`, entre otras.


## Despliegue del Proyecto

### Configurar y Ejecutar el Ecosistema Hadoop con Jupyter Notebooks

Antes de comenzar, asegúrate de tener Docker instalado en tu computadora. Una vez verificado esto, te recomiendo descargar todos los archivos del proyecto en un mismo directorio en tu entorno local. A continuación, utilizaremos la línea de comandos para iniciar nuestro ecosistema Hadoop y sus servicios.

Asegúrate de estar en la ruta donde se encuentra el archivo **docker-compose.yml** y ejecuta los siguientes comandos:

```bash
docker build -t hadoop-base docker/hadoop/hadoop-base && \
docker build -t hive-base docker/hive/hive-base && \
docker build -t spark-base docker/spark/spark-base && \
docker-compose up -d 
```

Estos comandos construyen las imágenes base necesarias a partir de los Dockerfiles y luego inician todos los contenedores simultáneamente.

Para verificar el estado de los contenedores, utiliza el siguiente comando:

```bash
docker ps
```

Deberías tener once servicios en ejecución:

- **hive-server**
- **hive-metastore**
- **namenode**
- **datanode**
- **spark-master**
- **spark-worker**
- **resourcemanager**
- **nodemanager**
- **kafka**
- **postgres**
- **historyserver**

Ten paciencia mientras todos los contenedores alcanzan el estado **healthy**. Este proceso puede llevar un tiempo, ya que cada contenedor necesita inicializarse correctamente y pasar las verificaciones de salud definidas. Una vez que todos estén en este estado, podrás proceder con confianza, sabiendo que el entorno está completamente operativo.

Después de haber confirmado que todo funciona correctamente, inicia el contenedor **kafka** con el siguiente comando:

```bash
docker exec -it kafka bash
```

Vamos a utilizar **Jupyter Notebooks**. Desde el contenedor, lanza el siguiente comando y espera a que Jupyter Notebook se inicialice:

```bash
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root
```

Luego, desde tu navegador, dirígete a la siguiente URL:

```plaintext
http://localhost:8888/lab
``` 

Con esto, tendrás acceso a Jupyter y te recomiendo que abras un nuevo notebook para comenzar a trabajar en el proyecto.

### Instrucciones de Uso

1. **Iniciar los servicios de Zookeeper y Kafka Server y creación del topico de Kafka**: Estos servicios se activarán automáticamente al lanzar el contenedor de Kafka. Asegúrate de crear el tema de Kafka con el siguiente comando:

   ```bash
   !kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic test
   ```

2. **Producción de mensajes**: Ejecuta el script `fake_data.py` para generar y enviar mensajes al tema de Kafka.

3. **Crear el directorio en HDFS**: Antes de continuar, asegúrate de que el directorio `/datalake/staging` exista en HDFS. Puedes crearlo con el siguiente comando:

   ```bash
   hdfs dfs -mkdir -p /datalake/staging
   ```

4. **Consumir mensajes de Kafka**: A continuación, ejecuta el script `ingest.py` para consumir los mensajes del tema de Kafka y almacenarlos en HDFS.

5. **Transformación y almacenamiento en Hive**: Finalmente, ejecuta el script `transform.py` para procesar los datos almacenados en HDFS y cargarlos en Hive.
