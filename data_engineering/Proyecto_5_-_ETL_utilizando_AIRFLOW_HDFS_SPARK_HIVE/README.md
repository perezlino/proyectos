# Proyecto de implementación de un pipeline para descargar, almacenar y procesar tasas de cambio de divisas (forex) de una API externa utilizando Python, HDFS, Spark y Hive

[![p389.png](https://i.postimg.cc/G2r1NDzk/p389.png)](https://postimg.cc/rdhbRKBm)

Este proyecto implementa un pipeline de procesamiento de datos utilizando Apache Airflow para descargar, almacenar y procesar tasas de cambio de divisas (forex) de una API externa. La arquitectura del pipeline está diseñada para ser escalable y modular, aprovechando diversos operadores de Airflow, incluidos sensores, operadores de Python, Bash, Hive y Spark.

## Características Principales

- **Descarga de Datos:** Utiliza un `HttpSensor` para verificar la disponibilidad de datos de tasas de cambio desde una API pública. También verifica la presencia de un archivo local que contiene las divisas base utilizando un `FileSensor`.
- **Procesamiento de Datos:** Emplea un `PythonOperator` para descargar y procesar las tasas de cambio, guardando los resultados en un archivo JSON.
- **Almacenamiento en HDFS:** Usa un `BashOperator` para mover los datos descargados a Hadoop Distributed File System (HDFS) para su almacenamiento y posterior acceso.
- **Integración con Hive:** Crea una tabla externa en Hive mediante un `HiveOperator` para almacenar las tasas de cambio, permitiendo el análisis y la consulta de los datos almacenados.
- **Procesamiento Distribuido con Spark**: Lee datos de tasas de cambio desde HDFS utilizando un `SparkSubmitOperator`. El job procesa los datos en formato JSON, eliminando filas duplicadas basadas en las columnas `base` y `last_update`, y llena los valores nulos con ceros. Finalmente, exporta el dataframe resultante a la tabla de Hive `forex_rates` para su almacenamiento y análisis posterior.

## Tecnologías

- **Apache Airflow**: Para la orquestación de flujos de trabajo.
- **HDFS**: Para el almacenamiento distribuido de datos.
- **Apache Hive**: Para el almacenamiento y consulta de datos en formato estructurado.
- **Apache Spark**: Para el procesamiento distribuido de datos.
- **Python**: Lenguaje de programación utilizado para el desarrollo de scripts y funciones personalizadas.

## Despliegue del proyecto

Para interactuar con estas herramientas, es necesario que los binarios correspondientes estén instalados en el mismo lugar donde está instalado Airflow. Este proyecto se ejecuta en Docker, y la imagen de Docker utilizada para ejecutar Airflow se construye a partir de múltiples imágenes. El flujo de construcción es el siguiente: 
```plaintext
OpenJDK → Hadoop → Hive → Spark → Airflow
```
Esto permite la interacción con Spark dentro del contenedor de Docker de Airflow, ya que la imagen de Airflow se deriva de la imagen de Spark, que a su vez se basa en la imagen de Hive, y así sucesivamente.

Asegúrate de que estos directorios estén ubicados en el mismo directorio que el archivo `docker-compose.yml`.

```plaintext
/
└── proyecto
    ├── docker/
    |     ├── airflow
    |     ├── hadoop
    |     ├── hive
    |     ├── postgres
    |     └── spark
    ├── mnt
    |     └── airflow/   
    |           ├── dags/  
    |           |    ├── files/
    |           |    |   └── forex_currencies.csv           # Archivo CSV con las monedas
    |           |    ├── scripts
    |           |    |   └── forex_processing.py            # Script para procesamiento en Spark
    |           |    └── forex_data_pipeline.py             # Archivo principal del DAG 
    |           └── airflow.cfg                             # Archivo de configuración de Airflow
    └── docker-compose.yaml  
```

Construye las imágenes base a partir de las cuales se basan los Dockerfiles y luego iniciar todos los contenedores a la vez:
```bash
docker build -t base-hadoop docker/hadoop/hadoop-base && \
docker build -t base-hive docker/hive/hive-base && \
docker build -t base-spark docker/spark/spark-base && \
docker-compose up -d --build
```

Deberíamos tener once servicios en ejecución: 

- **namenode**
- **datanode**
- **resourcemanager**
- **nodemanager**
- **hive-metastore**
- **hive-server**
- **hive-webhcat**
- **postgres**
- **spark-master**
- **spark-worker**
- **airflow**

Para verificar su estado, podemos utilizar el siguiente comando:

```bash
docker ps
```
Asegúrate de tener paciencia mientras todos los contenedores alcanzan el estado **healthy**. Este proceso puede llevar un tiempo, ya que cada contenedor necesita inicializarse correctamente y pasar las verificaciones de salud definidas. Una vez que todos estén en este estado, podrás proceder con confianza, sabiendo que el entorno está completamente operativo.

Con los servicios ya en funcionamiento, podemos acceder a la interfaz de usuario de Airflow utilizando las siguientes credenciales:

- **Usuario:** airflow
- **Contraseña:** airflow

Puedes ingresar a la UI en el siguiente enlace:

```plaintext
https://localhost:8080
```

Debemos ir a la pestaña **Admin > Connections** y luego vamos a agregar cuatro conexiones:

[![p373.png](https://i.postimg.cc/28TtzwBV/p373.png)](https://postimg.cc/KRkfftf2)
[![p374.png](https://i.postimg.cc/02ttgyLS/p374.png)](https://postimg.cc/JGk3ZM77) <ancho=575>

Configuramos la conexión con **HttpSensor** de la siguiente manera:

- **Conn Id:** forex_api
- **Conn Type:** HTTP
- **Host:** https://gist.github.com/

Además, debemos configurar otra conexión que se utilizará con el **FileSensor**, con los siguientes detalles:

- **Conn Id:** forex_path
- **Conn Type:** File (path)
- **Extra:** {"path":"/opt/airflow/dags/files"}

También tenemos que generar la conexión para el **HiveOperator**:

- **Conn Id:** hive_conn
- **Conn Type:** Hive Server2 Thrift
- **Host**: hive-server
- **Login:** hive
- **Password:** hive
- **Port:** 10000

Y una última conexión necesaria para el **SparkSubmitOperator**:

- **Conn Id:** spark_conn
- **Conn Type:** Spark
- **Host**: spark://spark-master
- **Port:** 7077

En la siguiente imagen se muestra el panel con todas las conexiones que vamos a utilizar:

[![p386.png](https://i.postimg.cc/0NhC2Ghw/p386.png)](https://postimg.cc/3yCGtG88)

Para revisar nuestros DAGs en Airflow, debemos hacerlo desde el contenedor **scheduler**. Para ello, podemos iniciar una sesión en dicho contenedor utilizando el siguiente comando:

```bash
docker exec -it <nombre_contenedor_scheduler> bash
```

Y luego dirigiendonos a la siguiente ruta:

```bash
cd /opt/airflow/dags
```

Para probar una tarea específica, debemos hacerlo dentro del contenedor **scheduler**. Desde la ruta `/opt/airflow/dags`, ejecuta el siguiente comando:

```bash
airflow tasks test <nombre_dag> <tarea> <una_fecha_en_formato_YYYY-mm-dd>
```

Una vez que hayas ejecutado nuestro DAG en la interfaz de usuario de Airflow y si la ejecución ha sido exitosa, deberías ver un resultado similar al que se muestra en la imagen a continuación:

[![p387.png](https://i.postimg.cc/1t6rgnZY/p387.png)](https://postimg.cc/vgYVkmTW)

Ahora debemos verificar el resultado. Empecemos por revisar la ubicación en HDFS donde se copio el archivo `forex_rates.json`:
```bash
docker exec -it namenode bash
```
Obtendremos algo similar a esto:
```bash
root@namenode:/#
root@namenode:/# hdfs dfs -ls /
Found 3 items
drwxr-xr-x   - airflow supergroup          0 2024-10-03 20:09 /forex
drwxrwxr-x   - root    supergroup          0 2024-10-03 19:32 /tmp
drwxr-xr-x   - root    supergroup          0 2024-10-03 19:32 /user
root@namenode:/# hdfs dfs -ls /forex
Found 1 items
-rw-r--r--   3 airflow supergroup        245 2024-10-03 20:09 /forex/forex_rates.json
``` 
Solo falta que revisemos la tabla creada en Hive para confirmar si el job de Spark ha cargado correctamente los datos en ella. Desde el contenedor **hive-server**:
```bash
beeline -u jdbc:hive2://localhost:10000
```
Ejecutamos las siguientes consultas:
```sql
show databases;
```  
```sql
show tables in default;
```  
```sql
select * from default.forex_rates;
```  
En la siguiente imagen se muestra la tabla creada en Hive:

[![p388.png](https://i.postimg.cc/L4DZxBv3/p388.png)](https://postimg.cc/23q66Ws1)

Detener todos los contenedores a la vez
```bash
docker-compose down
```
Volver a ejecutar todos los contenedores:
```bash
docker-compose up -d
```