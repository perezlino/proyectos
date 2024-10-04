# Proyecto de implementación de un pipeline para procesar y analizar información utilizando Python, Spark, PostgreSQL y Hive

[![p372.png](https://i.postimg.cc/nVRTWK4z/p372.png)](https://postimg.cc/rz4S0rYX)

Este proyecto implementa un pipeline de datos para procesar y analizar información de prescriptores utilizando Apache Spark. A través de este pipeline, se lleva a cabo la ingestión, limpieza, transformación y persistencia de datos, permitiendo generar reportes útiles para la toma de decisiones.

### Características

- **Ingestión de Datos**: Carga de archivos desde HDFS en formatos CSV y Parquet.
- **Validación de Datos**: Verificación de conteos y registros relevantes para asegurar la integridad de los datos cargados.
- **Limpieza de Datos**: Transformaciones y limpiezas aplicadas a los DataFrames de ciudades y prescriptores.
- **Transformación de Datos**: Generación de Dataframes finales.
- **Persistencia**: Almacenamiento de datos en Hive y PostgreSQL, asegurando que la información esté disponible para su análisis futuro.
- **Manejo de Logs**: Registro de eventos y errores para facilitar el monitoreo y la depuración.

### Tecnologias utilizadas

- `Python`
- `Apache Spark`
- `Hive`
- `PostgreSQL`
- `HDFS`

### Estructura del Proyecto

Este proyecto está diseñado para facilitar la gestión y procesamiento de datos en un pipeline de análisis. A continuación, se describen los principales componentes y scripts incluidos en la estructura del proyecto.

#### Scripts de Python

- **`get_all_variables.py`**: Establece y recupera las variables de entorno necesarias para la configuración del pipeline.
- **`create_objects.py`**: Contiene funciones para crear objetos de Spark.
- **`create_db.py`**: Creación de base de datos en Postgres.
- **`validations.py`**: Proporciona métodos para validar la calidad y consistencia de los datos.
- **`presc_run_data_ingest.py`**: Scripts para la ingestión de datos desde diversas fuentes.
- **`presc_run_data_preprocessing.py`**: Incluye funciones para la limpieza y preparación de los datos.
- **`presc_run_data_transform.py`**: Scripts para la transformación de datos y generación de reportes.
- **`presc_run_data_extraction.py`**: Métodos para extraer archivos en diferentes formatos.
- **`presc_run_data_persist.py`**: Funciones para la persistencia de datos en Hive y PostgreSQL.
- **`run_presc_pipeline.py`**: Orquesta el proceso completo del pipeline de datos.

#### Gestión de Directorios y Archivos

Estos scripts de shell facilitan la gestión de archivos y directorios en HDFS:

- **`deploy_directorios_hdfs.ksh`**: Despliega los directorios input en HDFS. En el caso de que existiesen los directorios "staging" se eliminarán y luego se volverán a crear. Se crea el directorio que actuará como base de datos en Hive. Con respecto a los directorios "output" en HDFS, estos se crearan automáticamente si no existen al momento de utilizar `df.write.save(path)` y especificar una ruta HDFS.
- **`copiar_archivos_locales_a_hdfs.ksh`**: Copia archivos desde el entorno local hacia el directorio de entrada "staging" en HDFS.
- **`eliminar_rutas_output_hdfs.ksh`**: Elimina los directorios "output" y las tablas finales de Hive (subdirectorios en el directorio de la base de datos) que se encuentran en HDFS. Esto con el fin de que al momento de volver a desplegar el pipeline no existan conflictos con estos directorios.
- **`copiar_archivos_en_hdfs_a_local.ksh`**: Borra los archivos en las rutas locales si existen y luego copia archivos desde HDFS hacia el entorno local.

### Despliegue del Proyecto

- **`deploy_proyecto_prescPipeline.ksh`**: **Despliega todo el proyecto** en el entorno correspondiente. Este archivo se utiliza exclusivamente para el despliegue general del proyecto y no está destinado a la gestión de directorios y archivos.

### Requerimientos del proyecto

Se solicitan 2 informes:

1.- **Informe de ciudades de EE. UU.**

   - Número de prescriptores distintos asignados para cada ciudad.
   - Total de TRX_CNT prescritos en cada ciudad.
   - Número de códigos postales en cada ciudad.
   - No reportar una ciudad si no hay prescriptor asignado.
   - **Tipo de archivo**: `json`
   - **Tipo de compresión**: `bzip2`

2.- **Informe de Prescriptores de EE. UU.**

   - Los 5 prescriptores con el mayor TRX_CNT en cada estado.
   - Considerar solo a los prescriptores con experiencia laboral de 20 a 50 años.
   - **Tipo de archivo**: `orc`
   - **Tipo de compresión**: `snappy`


### Despliegue del proyecto

Antes de comenzar asegúrate de tener instalado Docker en tu computador. Teniendo listo eso, te recomiendo descargar todos los archivos de este proyecto en un mismo directorio de tu entorno local. A continuación, utilizaremos la línea de comandos para iniciar nuestro ecosistema Hadoop y sus servicios. Ahora asegúrate de estar en la ruta donde se encuentra el archivo **docker-compose.yml** y ejecuta el siguiente comando:
```bash
docker compose up
```
Después, agrega el directorio **produccion** a un archivo zip y mueve el archivo **produccion.zip** al contenedor **spark-master**:
```bash
docker cp produccion.zip spark-master:/
```
Ahora arrancamos el contenedor **spark-master**:
```bash
docker exec -it spark-master bash
```
No es necesario instalar la herramienta **unzip** para descomprimir archivos, ya que, ya está configurada en el contenedor. Descomprimimos el archivo **produccion.zip**:
```bash
unzip produccion.zip
```
Luego, otorgamos permisos de ejecución a la ruta `/produccion/src/main/python` con el siguiente comando:
```bash
chmod -R +x /produccion/src/main/python
```
Nos posicionamos en la ruta `/produccion/src/main/python/bin`:
```bash
cd /produccion/src/main/python/bin
```
Y finalizamos con el siguiente comando para desplegar el Proyecto:
```bash
./deploy_proyecto_prescPipeline.ksh
```

### Verificar resultados en HDFS

Luego de ejecutarse nuestro pipeline, ejecutamos el siguiente comando para verificar que los datos fueron persistidos en HDFS:
```bash
hdfs dfs -ls /user/hive/warehouse/prescpipeline
```
Debiesemos ver algo similar a esto:
```bash
Found 2 items
drwxr-xr-x   - root supergroup          0 2024-07-18 22:10 /user/hive/warehouse/prescpipeline/df_city_final
drwxr-xr-x   - root supergroup          0 2024-07-18 22:11 /user/hive/warehouse/prescpipeline/df_presc_final
```

### Verificar resultados en PostgreSQL

Por medio de linea de comandos podemos verificar que se hayan creado las 2 tablas que creamos en este proyecto. Abrimos una nueva consola y comenzamos arrancando el contenedor **postgres**:
```bash
docker exec -it postgres bash
```
Ejecutamos el siguiente comando para ingresar a la base de datos `postgres_db`
```bash
psql -U admin --dbname postgres_db
```
Indicamos que queremos utilizar la base de datos **prescpipeline**:
```bash
\c prescpipeline
```
Luego, visualizamos las tablas que se han creado:
```bash
prescpipeline=# \dt
             List of relations
 Schema |      Name      | Type  |  Owner
--------+----------------+-------+---------
 public | df_city_final  | table | airflow
 public | df_presc_final | table | airflow
(2 rows)

```
Y finalmente, consultamos las tablas para verificar que efectivamente tengan datos:
```bash
prescpipeline=# SELECT * FROM df_presc_final LIMIT 10;
prescpipeline=# SELECT * FROM df_city_final LIMIT 10;
``` 

Para detener y eliminar todos los contenedores a la vez:
```bash
docker-compose down
```