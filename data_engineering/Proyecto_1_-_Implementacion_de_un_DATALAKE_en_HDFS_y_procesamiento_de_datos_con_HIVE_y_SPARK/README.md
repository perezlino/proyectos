# Proyecto de Implementación de un Data Lake en HDFS y posterior procesamiento de datos con Hive y Spark

<p align="center">
  <img src="https://i.postimg.cc/pV05D6zc/Screenshot-6.png" alt="Descripción de la imagen">
</p>

Este proyecto se centra en la creación e implementación de un **Data Lake** utilizando **Hadoop Distributed File System (HDFS)**, junto con el procesamiento de datos mediante **Hive** y **Spark**. El objetivo es estructurar y transformar grandes volúmenes de datos para facilitar el análisis y la generación de informes.

### Estructura del Proyecto

El proyecto consta de cinco archivos principales que gestionan la creación de la estructura del Data Lake y el procesamiento de datos:

1. **`deploy_directorios.sh`**: Este script se ejecuta en el contenedor **namenode** y se encarga de crear la estructura de carpetas en HDFS. Además, gestiona el movimiento de los archivos necesarios hacia HDFS.
2. **`deploy_esquema_landing_tmp.sql`**: Se ejecuta en el contenedor **hive-server** para desplegar el esquema de la capa **LANDING TMP** del Data Lake. Esta capa es crucial para la ingesta inicial de datos.
3. **`deploy_esquema_landing.sql`**: También ejecutado en el contenedor **hive-server**, este archivo despliega el esquema de la capa **LANDING**. Aquí se realiza un proceso de ETL (Extracción, Transformación y Carga) que implica capturar y binarizar los datos, así como aplicar compresión.
4. **`deploy_esquema_universal.sql`**: Este archivo despliega el esquema de la capa **UNIVERSAL** en el contenedor **hive-server**. En esta capa, se crea la tabla **`transaccion_enriquecida`**, la cual se construye utilizando las tablas **`persona`**, **`empresa`** y **`transaccion`**.
5. **`Spark`**: Utilizamos Spark para procesar y enriquecer los datos en la tabla **`transaccion_enriquecida`**, aplicando reglas de calidad genéricas y preparándolos para su uso en soluciones analíticas.

### Proceso de Transformación

Crearemos 4 capas:

- **Paso 1.** Capturar los datos dentro de nuestro estorno de BIG DATA (`LANDING TEMP`)
- **Paso 2.** Convertirlo a un formato de rápido procesamiento (binarizar los archivos) (`LANDING`)    
- **Paso 3.** Modelamiento y Aplicación reglas de calidad: Transformación y almacenamiento (`UNIVERSAL`)
- **Paso 4.** Procesamiento analítico avanzado y generación de insights (`SMART`)

### Despliegue del proyecto

Para comenzar, te recomiendo descargar todo el contenido en formato ZIP en tu entorno local. A continuación, utilizaremos la línea de comandos para iniciar nuestro ecosistema Hadoop y sus servicios. Ahora asegúrate de estar en la ruta donde se encuentra el archivo **docker-compose.yml** y ejecuta el siguiente comando:

```bash
docker compose up
```

Ahora nos disponemos a mover archivos desde nuestro entorno local al contenedor **namenode**. Ubicate en el directorio raiz del proyecto y ejecuta los siguientes comandos

```bash
docker cp ./data/persona.data namenode:/hadoop-data/archivos
docker cp ./data/empresa.data namenode:/hadoop-data/archivos
docker cp ./data/transacciones.data namenode:/hadoop-data/archivos
```

```bash
docker cp ./data/definicion_de_esquema/persona.avsc namenode:/hadoop-data/schemas
docker cp ./data/definicion_de_esquema/empresa.avsc namenode:/hadoop-data/schemas
docker cp ./data/definicion_de_esquema/transaccion.avsc namenode:/hadoop-data/schemas
```

```bash
docker cp deploy_directorios.sh namenode:/hadoop-data/deploy
```

Estamos listos para desplegar la estructura de directorios y el movimiento de archivos en HDFS. Ahora arrancamos el contenedor **namenode**:

```bash
docker exec -it namenode bash
```

Y ejecutamos el siguiente comando:

```bash
sh /hadoop-data/deploy/deploy_directorios.sh proyectos datalake
```

Si tenemos éxito nos devolverá la siguiente respuesta :

```bash
Eliminando carpeta raiz...
Creando la estructura de carpetas para landing_tmp...
Creando la estructura de carpetas para landing...
Creando la estructura de carpetas para universal...
Creando la estructura de carpetas para smart...
Subiendo archivos de datos...
Subiendo archivos de schema...
```

Para verificar que nuestra estructura de directorios fue creada debemos revisar HDFS, para ello ejecuta el siguiente comando:

```bash
hdfs dfs -ls /user/proyectos/datalake/database
```

También podemos ejecutar los siguientes comandos para verificar que los archivos y schemas fueron movidos hacia HDFS:

```bash
hdfs dfs -ls /user/proyectos/datalake/archivos
```

```bash
hdfs dfs -ls /user/proyectos/datalake/schema/landing
```

A continuación nos disponemos a mover los archivos de despliegue de base de datos y tablas desde nuestro entorno local al contenedor **hive-server**. Ubicate en el directorio raiz del proyecto y ejecuta los siguientes comandos:

```bash
docker cp deploy_esquema_landing_tmp.sql hive-server:/opt/hive/deploy
docker cp deploy_esquema_landing.sql hive-server:/opt/hive/deploy
docker cp deploy_esquema_universal.sql hive-server:/opt/hive/deploy
```

Estamos preparados para la segunda parte de nuestro proyecto que consiste desplegar la creación de bases de datos y tablas de las capas `LANDING_TMP`, `LANDING` Y `UNIVERSAL` de nuestro Data Lake en Hive. Ahora arrancamos el contenedor **hive-server**:

```bash
docker exec -it hive-server bash
```

Y ejecutamos los siguientes comandos en el orden que se indica:

```bash
beeline -u jdbc:hive2://localhost:10000 -f /opt/hive/deploy/deploy_esquema_landing_tmp.sql --hiveconf "PARAM_RAIZ=proyectos" --hiveconf "PARAM_PROYECTO=datalake"
```

```bash
beeline -u jdbc:hive2://localhost:10000 -f /opt/hive/deploy/deploy_esquema_landing.sql --hiveconf "PARAM_RAIZ=proyectos" --hiveconf "PARAM_PROYECTO=datalake"
```

```bash
beeline -u jdbc:hive2://localhost:10000 -f /opt/hive/deploy/deploy_esquema_universal.sql --hiveconf "PARAM_RAIZ=proyectos" --hiveconf "PARAM_PROYECTO=datalake"
```

Ahora entramos a la tercera y última parte de nuestro proyecto, donde el requerimiento es construir la tabla **TRANSACCION_ENRIQUECIDA**  en función de las tablas `UNIVERSAL.PERSONA`, `UNIVERSAL.EMPRESA` y `UNIVERSAL.TRANSACCION`". Los campos de la tabla son los siguientes:

```text
               _____________________________________________________________________________________
              |        CAMPO        |  TIPO  |                    DESCRIPCION                       |
              |---------------------|--------|------------------------------------------------------|
              | ID_PERSONA          | INT    | ID de la persona que realizo la transaccion          |
              |---------------------|--------|------------------------------------------------------|
              | NOMBRE_PERSONA      | STRING | Nombre de la persona que realizó la transaccion      |
              |---------------------|--------|------------------------------------------------------|
              | EDAD_PERSONA        | INT    | Edad de la persona que realizó la transaccion        |
              |---------------------|--------|------------------------------------------------------|
              | SALARIO_PERSONA     | DOUBLE | Salario de la persona que realizó la transaccion     |
              |---------------------|--------|------------------------------------------------------|
              | TRABAJO_PERSONA     | STRING | Nombre de la empresa donde trabaja la persona        |
              |---------------------|--------|------------------------------------------------------|
              | MONTO_TRANSACCION   | DOUBLE | Monto de la transaccion realizada                    |
              |---------------------|--------|------------------------------------------------------|
              | FECHA_TRANSACCION   | STRING | Fecha de la transaccion realizada                    |
              |---------------------|--------|------------------------------------------------------|
              | EMPRESA_TRANSACCION | STRING | Nombre de la empresa donde se realizó la transaccion |
              |_____________________|________|______________________________________________________|
```

A continuación, se presenta un diagrama de proceso que ilustra los pasos necesarios para obtener la tabla **TRANSACCION_ENRIQUECIDA**. Este diagrama detalla las etapas involucradas en el proceso de su construcción:

<p align="center">
  <img src="https://i.ibb.co/pxcpfvL/p369.png" alt="Descripción de la imagen">
</p>

En la capa **UNIVERSAL** se creó la tabla **TRANSACCION_ENRIQUECIDA**, sin embargo, solo se hizo con fines educativos y desde Spark vamos a truncar dicha tabla y volver a construirla. Por tanto, para ello vamos a mover el script de despliegue en Spark desde nuestro entorno local al contenedor **spark-master**. Ubicate en el directorio raiz del proyecto y ejecuta el siguiente comando:

```bash
docker cp deploy_script_spark.py spark-master:/usr/local/spark/deploy
```

Ahora arrancamos el contenedor **spark-master**:

```bash
docker exec -it spark-master bash
```

Antes de finalizar el proyecto desplegando el script de spark por linea de comandos, podriamos confirmar si las propiedades que vamos a lanzar estan o no habilitadas. Esto podemos hacerlo desde la consola **pyspark**.  Ejecutamos el siguiente comando:

```bash
pyspark
```

Despues de cargar la consola **pyspark**, lanzamos las siguientes propiedades para ver si estan habilitadas o no:

```python
spark.sql("SET hive.exec.dynamic.partition").show()
```

```python
spark.sql("SET hive.exec.dynamic.partition.mode").show()
```

En caso de no estar habilitadas las debemos habilitar en nuestro script, dado que ambas propiedades nos permiten realizar particiones dinámicas en Hive a través de Spark. Finalmente, salimos de la consola **pyspark** y ejecutamos el script utilizando el siguiente comando:

```bash
spark-submit /usr/local/spark/deploy/deploy_script_spark.py
```
Y eso eso es todo!. Hemos creado un `Data Lake` sobre **HDFS** el cual nos permitirá procesar multiples archivos utilizando **Hive** y **Spark**.

___

###  Crear reportes utilizando la tabla **TRANSACCION_ENRIQUECIDA** utilizando Jupyter Notebooks

Es importante instalar Jupyter en Docker (en el contenedor `spark-master`) e integrar Spark 3.* con Jupyter. En dicho contenedor ya fue agregado el puerto **32774:8888** para trabajar con Jupyter Notebooks. 

Comencemos instalando actualizaciones dentro del contenedor:
```bash
apt update
```
Instalamos actualizaciones con respecto al comando **pip**
```bash
pip install --upgrade pip
```
Instalamos **Jupyter Notebooks**
```bash
pip install jupyterlab
```
A continuación, ejecutamos el siguiente comando:
```bash
jupyter lab --ip 0.0.0.0 --allow-root
```
Copiamos el enlace que nos aparece y lo pegamos en el navegador. Debemos cambiar el puerto `8888` por el puerto que indicamos en el archivo `docker-compose.yaml`, en nuestro caso, por el puerto `32774`. Y ya podemos acceder a Jupyter Notebooks. Ahora solo nos falta integrar Spark con esta herramienta.
```bash
# Ejemplo
http://localhost:32774/lab?token=8664e8f7ed7f7e003a3fc258d3b034dde2861d9e25cdec2b
```
Luego, abrimos una nueva consola (ya que la anterior esta utilizada por Jupyter y no podemos escribir) para ejecutar el contenedor `spark-master` para lanzar el siguiente comando que nos permite visualizar los kernel activos para Jupyter:
```bash
jupyter kernelspec list
```
Vamos a crear un directorio para albergar el nuevo kernel Spark 3.*
```bash
mkdir /usr/local/share/jupyter/kernels/pyspark3
```
Luego, nos ubicamos en la ruta del directorio raiz del proyecto y copiamos el archivo `kernel.json` desde nuestro sistema local al contenedor **spark-master** en la ruta especificada:
```bash
docker cp kernel.json spark-master:/usr/local/share/jupyter/kernels/pyspark3
```
Procedemos a instalar el kernel que creamos:
```bash
jupyter kernelspec install /usr/local/share/jupyter/kernels/pyspark3 --user
```
Verificamos que exista nuestro kernel para Spark 3.*:
```bash
jupyter kernelspec list
```
Refrescamos Jupyter en el navegador e iniciamos una sesión de spark sencilla solo con el fin de comprobar que todo funciona como esperamos:
```python
from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Test") \
    .getOrCreate()

# Mostrar la versión de Spark
print("Versión de Spark:", spark.version)

# Hacer algunas operaciones sencillas
data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
columns = ["Nombre", "ID"]

# Crear un DataFrame
df = spark.createDataFrame(data, columns)

# Mostrar el DataFrame
df.show()
```
Y asi luego podemos ejecutar los reportes propuestos en el notebook **notebook_reportes.ipynb** sobre la tabla **TRANSACCION_ENRIQUECIDA**.