# Proyecto de Procesamiento de Datos en tiempo real con Hadoop, Kafka y Spark

[![398.png](https://i.postimg.cc/4NXf7M85/398.png)](https://postimg.cc/MXr2gsJM)

Este proyecto configura un ecosistema de procesamiento de datos en tiempo real utilizando Hadoop, Kafka y Spark, todo gestionado a través de Docker y accesible mediante Jupyter Notebooks. La solución permite la generación, producción y consumo de logs, facilitando el análisis y procesamiento de datos en tiempo real.

## Características Principales

- **Ecosistema Completo**: Implementa Hadoop, Kafka, Spark, y Jupyter Notebooks en contenedores Docker para un entorno de trabajo aislado y fácil de manejar.
- **Generación de Logs**: Utiliza una aplicación simuladora para generar logs, que se envían a Kafka para su procesamiento.
- **Integración con Kafka**: Permite la creación y gestión de tópicos, así como la producción y consumo de mensajes en tiempo real.
- **Análisis de Datos en Tiempo Real**: A través de Spark Structured Streaming, los datos se procesan y se almacenan en HDFS, proporcionando una solución robusta para análisis de grandes volúmenes de información.

## Estructura del Proyecto

El proyecto se organiza en varias secciones que abarcan desde la configuración del entorno hasta el procesamiento de datos:

1. **Despliegue del Ecosistema**: Instrucciones para configurar y ejecutar todos los servicios necesarios.
2. **Preparación de la Fuente de Streaming**: Clonación y configuración de la aplicación `gen_logs` para la generación de logs.
3. **Configuración de Kafka**: Creación de tópicos, producción y consumo de logs utilizando herramientas de Kafka.
4. **Ejecución de Scripts de Procesamiento**: Utilización de Jupyter Notebooks para ejecutar scripts de Spark que procesan los datos streaming desde Kafka y los almacenan en HDFS.

## Tecnologías utilizadas

- Hadoop
- HDFS
- Kafka Connect
- Spark Structured Streaming

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

___

### Preparar nuestra Fuente Streaming

Nuestra fuente de streaming será una aplicación llamada `gen_logs`, ubicada en `/opt`. Esta aplicación actúa como un simulador para la generación de logs. Para comenzar, gestionaremos la clonación del siguiente repositorio de Git utilizando un notebook:

```python
!git clone https://github.com/dgadiraju/gen-logs-python3.git
```

Una vez clonado, moveremos el directorio **gen_logs** a la ruta `/opt`:

```python
!mv -f gen-logs-python3/gen_logs /opt
```

Después, eliminaremos el directorio clonado **gen-logs-python3** para mantener nuestro entorno limpio:

```python
!rm -rf gen-logs-python3
```

A continuación, realizaremos los siguientes pasos desde una nueva consola que deberás abrir, ya que la primera consola está inactiva debido a la ejecución de Jupyter Notebooks en ella. Lanza nuevamente el contenedor **kafka**:

```bash
docker exec -it kafka bash
```

Añadiremos la ruta `/opt/gen_logs` a la variable `PATH`, lo que facilitará el acceso a los archivos de la aplicación. Notar que este paso en particular debemos realizarlo en la consola de Jupyter y en todas las consolas que utilicemos durante el despliegue del proyecto:

```bash
export PATH=$PATH:/opt/gen_logs
```

En el directorio /opt/gen_logs tenemos los siguientes archivos y directorios:
```plaintext
/opt/gen_logs
├── data
├── lib
├── logs
│   └── access.log
├── start_logs.sh
├── stop_logs.sh
└── tail_logs.sh
```

Para comenzar a generar logs web en el contenedor debemos ejecutar el siguiente script:
```bash
start_logs.sh
```

Para validar si logs se están generando ejecuta el siguiente script: (Presiona `Ctrl + C` para salir)
```bash
tail_logs.sh
``` 

Para detener la generación de logs web ejecuta el siguiente script:
```bash
stop_logs.sh
```

___

### `Paso 1 - Opción 1`: Configurar la Fuente Streaming para producir y consumir logs con Kafka Console

Una vez que `gen_logs` esté configurado, podemos redirigir la salida de `tail_logs.sh` al topico de Kafka.

Primero, creemos un topico llamado `retaildg`:
```bash
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic retaildg
```

Para validar si el topico se creó, vamos a buscarlo entre los topicos existentes:
```bash
kafka-topics.sh --list --bootstrap-server localhost:9092 --topic retaildg
```

Podemos obtener más detalles especificos del topico con el siguiente comando:
```bash
kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic retaildg
```

Ahora, vamos a redirigir la salida de `tail_logs.sh` al topico de Kafka por medio de **kafka-console-producer**:
```bash
tail_logs.sh|kafka-console-producer.sh --bootstrap-server localhost:9092 --topic retaildg
```

Desde otra consola podemos ejecutar **kafka-console-consumer** para consumir mensajes del topico **retaildg**. Lanza el contenedor **kafka** y ejecuta esto usando la terminal:
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic retaildg --from-beginning
```

Tener en cuenta que para salir ya sea del **kafka-console-producer** como de **kafka-console-consumer** debemos pulsar `CTRL + C`.

Si quieres eliminar el topico ejecuta el siguiente comando (Demora unos segundos en eliminarse):
```bash
kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic retaildg
```
___

### `Paso 1 - Opción 2`: Configurar la Fuente Streaming para producir con Kafka Connect y consumir logs con Kafka Console

Una vez que `gen_logs` esté configurado, podemos redirigir la salida de `tail_logs.sh` al topico de Kafka.

Vamos a comenzar creando el topico llamado `retaildg`:
```bash
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic retaildg
```

Para validar si el topico se creó, vamos a buscarlo entre los topicos existentes:
```bash
kafka-topics.sh --list --bootstrap-server localhost:9092 --topic retaildg
```

Podemos obtener más detalles especificos del topico con el siguiente comando:
```bash
kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic retaildg
```

Crearemos un directorio de trabajo `~/kafka_connect/retail_logs_produce`:
```bash
mkdir -p ~/kafka_connect/retail_logs_produce
```

Nos enfocaremos en los archivos `connect-standalone.properties` y `connect-file-source.properties`, ubicados en la ruta `/kafka/config`. Estos son los archivos que utilizaremos para iniciar Kafka Connect en modo standalone, permitiéndonos leer datos de un archivo de log en un tópico de Kafka.

Voy a copiar y renombrar el archivo `connect-standalone.properties` como `retail_logs_standalone.properties`, y el archivo `connect-file-source.properties` como `retail_logs_file_source.properties`.

Vamos a copiar los siguientes archivos de la carpeta de configuración de Kafka:
```bash
cp /opt/kafka/config/connect-standalone.properties ~/kafka_connect/retail_logs_produce/retail_logs_standalone.properties

cp /opt/kafka/config/connect-file-source.properties ~/kafka_connect/retail_logs_produce/retail_logs_file_source.properties
```

A continuación, procederemos a modificar el archivo `retail_logs_standalone.properties` ubicado en la ruta `/root/kafka_connect/retail_logs_produce`. Vamos a abrirlo en Jupyter Notebooks:
```properties
bootstrap.servers=localhost:9092
key.converter=org.apache.kafka.connect.storage.StringConverter 
value.converter=org.apache.kafka.connect.storage.StringConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true
offset.storage.file.filename=/root/kafka_connect/retail_logs_produce/retail.offsets
offset.flush.interval.ms=10000
```
[![p395.png](https://i.postimg.cc/B601vP8n/p395.png)](https://postimg.cc/xk6dxCyw)

Y también modificamos el archivo `retail_logs_file_source.properties`:
```properties
name=local-file-source
connector.class=FileStreamSource
tasks.max=1
file=/opt/gen_logs/logs/access.log
topic=retaildg
```

[![p396.png](https://i.postimg.cc/ZnYB8kxk/p396.png)](https://postimg.cc/hzwtn5YM)

Si haz estado mucho tiempo con el generador de logs es posible que el archivo `access.log` haya crecido mucho y podrías encontrarte con problemas de memoria insuficiente, por lo que te recomiendo que resetees el archivo `access.log`. Te recomendaría encarecidamente que ejecutes los comandos para asegurarte de que el archivo `access.log` esté reseteado antes de iniciar el proceso de Kafka Connect. 

```bash
stop_logs.sh
cat /dev/null > /opt/gen_logs/logs/access.log
start_logs.sh
tail_logs.sh
```

Regresa a la segunda consola que habíamos abierto y navega al directorio donde configuramos los archivos de propiedades de Kafka Connect para producir los datos en el tópico de Kafka:

```bash
cd /root/kafka_connect/retail_logs_produce
```

Nos devuelve algo asi:
```bash
root@kafka:~/kafka_connect/retail_logs_produce# ls -ltr
total 8
-rw-r--r-- 1 root root 2307 Oct  8 19:01 retail_logs_standalone.properties
-rw-r--r-- 1 root root  898 Oct  8 19:01 retail_logs_file_source.properties
```

Ejecutemos Kafka Connect para producir mensajes en el topico de Kafka:
```bash
connect-standalone.sh retail_logs_standalone.properties retail_logs_file_source.properties
```

Una vez que los datos se han enviado al topico de Kafka, podemos comenzar a consumirlos utilizando **`kafka-console-consumer`**. Desde una tercer consola podemos ejecutar **kafka-console-consumer** para consumir mensajes del topico **retaildg**. Lanza el contenedor **kafka** y ejecuta esto usando la terminal:

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic retaildg --from-beginning
```

Antes de ejecutar el script de Spark Structured Streaming, crearé una ruta para el directorio principal en HDFS llamado `/proyecto/kafka/retail_logs/gen_logs`. Dado que tenemos las tres consolas utilizadas, abriremos una cuarta y primero eliminare la ruta de directorios si existen y luego las volveré a crear:
```bash
hdfs dfs -rm -R -skipTrash /proyecto/kafka/retail_logs/gen_logs
hdfs dfs -mkdir -p /proyecto/kafka/retail_logs/gen_logs
```

Verificamos que se haya creado el directorio:
```bash
root@kafka:/# hdfs dfs -ls /proyecto/kafka/retail_logs
Found 1 items
drwxr-xr-x   - root supergroup          0 2024-10-08 22:57 /proyecto/kafka/retail_logs/gen_logs
```

Tener en cuenta que para salir ya sea del **kafka connect producer** como de **kafka-console-consumer** debemos pulsar `CTRL + C`.

Si haz estado mucho tiempo con el generador de logs es posible que el archivo `access.log` haya crecido mucho y podrías encontrarte con problemas de memoria insuficiente, por lo que te recomiendo que resetees el archivo `access.log`. Te recomendaría encarecidamente que ejecutes los comandos para asegurarte de que el archivo `access.log` esté reseteado antes de iniciar el proceso de Kafka Connect. 

```bash
stop_logs.sh
cat /dev/null > /opt/gen_logs/logs/access.log
start_logs.sh
tail_logs.sh
```

Si quieres eliminar el topico ejecuta el siguiente comando (Demora unos segundos en eliminarse):
```bash
kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic retaildg
```

Y luego volver a ejecutar el proceso de producción y consumo.

___

### `Paso 2`: Ejecución de notebook con script Spark Structured Streaming para el procesamiento de datos streaming desde un topico de Kafka y posterior almacenamiento en HDFS

Volvemos a Jupyter Notebook, subimos el notebook que adjunte y luego lanzamos el script de Spark de manera ordenada.