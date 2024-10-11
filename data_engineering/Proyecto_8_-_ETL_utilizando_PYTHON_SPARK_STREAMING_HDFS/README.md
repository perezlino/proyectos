# Proyecto de Análisis de Carga Incremental con Spark Structured Streaming

[![p468.png](https://i.postimg.cc/Z5ZzZfqp/p468.png)](https://postimg.cc/pp06JJzL)

## Descripción

Este proyecto se enfoca en el análisis de carga incremental de datos utilizando **Apache Spark** y su módulo **Structured Streaming**. El objetivo principal es descargar archivos CSV de actividad de GitHub (GH Archive), almacenarlos en HDFS y analizar los datos de forma incremental a través de cuatro notebooks distintos, cada uno utilizando diferentes enfoques de carga.

## Características Principales:

- **Configuración de Spark**: Se establece una sesión de Spark con soporte para Hive y configuraciones personalizadas que optimizan la manipulación de datos en streaming.

- **Descarga de Datos**: Se implementa una función que permite descargar archivos de datos desde un repositorio de GitHub y almacenarlos en un sistema de archivos local. Posteriormente, estos archivos se suben a HDFS, organizándose en una estructura de carpetas que refleja la fecha de creación (año, mes, día).

- **Análisis de Carga Incremental**: El proyecto incluye cuatro notebooks, cada uno abordando diferentes métodos de carga incremental. Esto permite explorar cómo Spark Structured Streaming puede gestionar y procesar datos en tiempo real, optimizando el análisis y la eficiencia.

- **Particionamiento y Checkpoints**: Los datos se almacenan en HDFS en particiones basadas en la fecha, garantizando un acceso eficiente y organizado. Además, se establecen ubicaciones específicas para los checkpoints, asegurando la durabilidad y recuperación de datos en caso de fallos.

## Tecnologías Utilizadas:

- **Spark Structured Streaming**
- **HDFS**
- **Jupyter Notebooks**
- **Python**

# Estructura del Proyecto:

- **docker/**: Este directorio incluye los servicios de Hadoop, Hive, Spark y PostgreSQL, facilitando el entorno necesario para el procesamiento y análisis de datos. 

- **notebooks/**: Contiene los notebooks de Jupyter donde se realiza el análisis de carga incremental y se aplican diferentes técnicas de procesamiento.

- **docker-compose.yaml**: También incluye el archivo `docker-compose.yaml` para la configuración y despliegue de estos servicios.

## Despliegue del Proyecto

### Configurar y Ejecutar el Ecosistema Hadoop con Jupyter Notebooks

Antes de comenzar, asegúrate de tener Docker instalado en tu computadora. Una vez verificado esto, te recomiendo descargar todos los archivos del proyecto en un mismo directorio en tu entorno local. A continuación, utilizaremos la línea de comandos para iniciar nuestro ecosistema Hadoop y sus servicios.

Asegúrate de que estos directorios estén ubicados en el mismo directorio que el archivo `docker-compose.yml`.

```plaintext
/
└── proyecto
    ├── docker/
    |     ├── hadoop
    |     ├── hive
    |     ├── postgres
    |     └── spark
    ├── notebooks/    
    └── docker-compose.yaml  
```

Posicionate en la ruta donde se encuentra el archivo **docker-compose.yml** y ejecuta los siguientes comandos:

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

Deberías tener diez servicios en ejecución:

- **hive-server**
- **hive-metastore**
- **namenode**
- **datanode**
- **spark-master**
- **spark-worker**
- **resourcemanager**
- **nodemanager**
- **postgres**
- **historyserver**

Ten paciencia mientras todos los contenedores alcanzan el estado **healthy**. Este proceso puede llevar un tiempo, ya que cada contenedor necesita inicializarse correctamente y pasar las verificaciones de salud definidas. Una vez que todos estén en este estado, podrás proceder con confianza, sabiendo que el entorno está completamente operativo.

Después de haber confirmado que todo funciona correctamente, inicia el contenedor **spark-master** con el siguiente comando:

```bash
docker exec -it spark-master bash
```

Vamos a utilizar **Jupyter Notebooks**. Desde el contenedor, lanza el siguiente comando y espera a que Jupyter Notebook se inicialice:

```bash
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root
```

Luego, desde tu navegador, dirígete a la siguiente URL:

```plaintext
http://localhost:32774/lab
``` 

Con esto, tendrás acceso a Jupyter y te recomiendo que abras un nuevo notebook para comenzar a trabajar en el proyecto.

___

### Construir estructura de directorios del proyecto

Vamos a crear un directorio de trabajo en HDFS. Crearé un directorio principal llamado `streaming` en el directorio base. En esa carpeta `streaming`, se crearán dos directorios principales que representarán dos diferentes capas, que generalmente utilizamos en los pipelines de procesamiento de datos. Las nombraremos `landing` y `bronze`.

La carpeta `landing` contendrá archivos de datos CSV que se descargaron de la fuente. Luego, crearemos otra carpeta llamada `bronze`. El job de Spark Structured Streaming leerá los datos desde la carpeta `landing` y procesará los datos por año, mes y día, y los escribirá en la carpeta `bronze`.

El directorio base que estoy utilizando es `/proyecto/spark`. A continuación, procederemos a crear la estructura de directorios en HDFS. Puedes ejecutar los siguientes comandos en una terminal o directamente en un notebook de Jupyter; personalmente, prefiero el segundo método:

```python
!hdfs dfs -rm -R -skipTrash /proyecto/spark/streaming/landing
!hdfs dfs -mkdir -p /proyecto/spark/streaming/landing
```

```python
!hdfs dfs -rm -R -skipTrash /proyecto/spark/streaming/bronze
!hdfs dfs -mkdir -p /proyecto/spark/streaming/bronze
```

Para validar la creación de la carpeta, copia la ruta y utiliza el siguiente comando `hdfs dfs -ls`. Es importante mencionar que no verás nada en la carpetas `landing` y `bronze` porque acaba de ser creada y aún no hemos añadido ningún archivo o subcarpeta:

```python
!hdfs dfs -ls /proyecto/spark/streaming
```
___

### Transferencia de Notebooks al Contenedor Spark

A continuación, realizaremos la transferencia de los notebooks desde nuestro entorno local al contenedor **spark-master**. Asegúrate de estar en el directorio raíz del proyecto y ejecuta los siguientes comandos:

```bash
docker cp notebooks/carga_2024_10_06.ipynb spark-master:/
docker cp notebooks/carga_2024_10_07.ipynb spark-master:/
docker cp notebooks/carga_2024_10_08.ipynb spark-master:/
docker cp notebooks/carga_2024_10_09.ipynb spark-master:/
```

Una vez transferidos, procederemos a ejecutar cada notebook de manera ordenada desde Jupyter.

## Conclusión

Este proyecto es ideal para quienes desean profundizar en el análisis de datos en tiempo real y comprender las capacidades de Spark Structured Streaming en la gestión de grandes volúmenes de información. Al explorar diversas formas de carga incremental, los usuarios pueden aprender a optimizar sus flujos de trabajo y mejorar la eficiencia en el análisis de datos.