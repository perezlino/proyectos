# Proyecto de implementación de un pipeline para procesar y analizar información utilizando Python, Spark, PostgreSQL y Hive

Este proyecto implementa un pipeline de datos para procesar y analizar información de prescriptores utilizando Apache Spark. A través de este pipeline, se lleva a cabo la ingestión, limpieza, transformación y persistencia de datos, permitiendo generar reportes útiles para la toma de decisiones.

### Características

- **Ingestión de Datos**: Carga de archivos desde HDFS en formatos CSV y Parquet.
- **Validación de Datos**: Verificación de conteos y registros relevantes para asegurar la integridad de los datos cargados.
- **Limpieza de Datos**: Transformaciones y limpiezas aplicadas a los DataFrames de ciudades y prescriptores.
- **Transformación de Datos**: Generación de reportes sobre ciudades y los cinco principales prescriptores.
- **Persistencia**: Almacenamiento de datos en Hive y PostgreSQL, asegurando que la información esté disponible para su análisis futuro.
- **Manejo de Logs**: Registro de eventos y errores para facilitar el monitoreo y la depuración.

### Requisitos

- Python
- Apache Spark
- PostgreSQL
- Hive

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

Antes de comenzar asegúrate de tener instalado Docker en tu computador. Teniendo listo eso, te recomiendo descargar el directorio `docker` y el archivo `docker-compose.yml` a tu entorno local, ambos deben estar en la misma ruta. A continuación, utilizaremos la línea de comandos para iniciar nuestro ecosistema Hadoop y sus servicios. Ahora asegúrate de estar en la ruta donde se encuentra el archivo **docker-compose.yml** y ejecuta el siguiente comando:

```bash
docker compose up
```

Ahora arrancamos el contenedor **spark-master**:

```bash
docker exec -it spark-master bash
```

Y ejecutamos el siguiente comando para desplegar el Proyecto:

```bash
./produccion/src/main/python/bin/deploy_proyecto_prescPipeline.ksh
```
