# Data Pipeline para el Análisis del Titanic utilizando Airflow, Pandas y PostgreSQL

[![p377.png](https://i.postimg.cc/jSh72RVh/p377.png)](https://postimg.cc/RWNFb5Z3)

Este proyecto implementa un pipeline de datos utilizando Apache Airflow para procesar un conjunto de datos sobre los pasajeros del Titanic. El pipeline realiza las siguientes tareas:

## Descripción del Proyecto

1.- **Descarga de Datos**:

- Se descarga un archivo CSV que contiene información sobre los pasajeros del Titanic desde un repositorio en GitHub.

2.- **Calidad de los Datos**:

- Se genera un informe de calidad de datos utilizando la libreria `pandas_profiling`. Este informe proporciona un análisis exhaustivo de los datos, identificando valores faltantes y describiendo las características de las columnas.

3.- **Curación de Datos**:

- Se procesan los datos para eliminar columnas innecesarias y manejar valores faltantes.
- Se crean nuevas columnas derivadas, como "Full Name" y "Title", y se simplifican los títulos de los pasajeros.
- Los datos curados se guardan en un nuevo archivo CSV.

4.- **Carga en Bases de Datos**:

- Los datos se cargan en una base de datos PostgreSQL. Se crean dos tablas: `raw_titanic` para almacenar los datos originales y `master_titanic` para los datos procesados.
- La tabla `master_titanic` también realiza transformaciones adicionales, como clasificar a los pasajeros en grupos demográficos.

5.- **Validación**:

- Se utiliza un `SensorSQL` para validar que los datos se hayan cargado correctamente en la tabla `master_titanic`.

## Estructura del Proyecto

- `dags/`: Contiene el archivo de definición del DAG.
- `data/`: Directorio para almacenar archivos CSV.
- `sql/`: Contiene scripts SQL para la creación de tablas y carga de datos.
- `README.md`: Este archivo.

## Tecnologías utilizadas

- Apache Airflow
- Python / Pandas
- PostgreSQL

## Despliegue del proyecto

Al utilizar el archivo `docker-compose.yml` para levantar Airflow, es necesario crear los directorios `dags`, `plugins` y `logs`. Puedes hacerlo ya sea mediante la consola de Bash o manualmente:
```bash
mkdir ./dags ./plugins ./logs
```
Asegúrate de que estos directorios estén ubicados en el mismo directorio que el archivo `docker-compose.yml`.
```plaintext
/
└── proyecto
    ├── dags
    ├── logs
    ├── plugins
    └── docker-compose.yaml    
```

También crearemos dos subdirectorios dentro del directorio `dags`:

```bash
mkdir -p ./dags/data ./dags/sql
```

Debemos crear el script de Python **dag.py** en el directorio **dags**, donde definiremos nuestro DAG.

```plaintext
/
└── dags
    ├── data
    ├── sql
    └── dag.py
```

A continuación, creamos un **Dockerfile** para instalar dos librerias necesarias en el contenedor. Para ello, utilizaremos el archivo **requirements.txt**, en el cual se definen dichas librerias.
```plaintext
/
└── proyecto
    ├── dags
    ├── logs
    ├── plugins
    ├── docker-compose.yaml 
    ├── Dockerfile        
    └── requirements.txt   
```

Debido a que hemos creado un Dockerfile con actualizaciones, es necesario construir una nueva imagen. Para ello ejecutamos el siguiente comando:

```bash
docker build -t extending_airflow:latest202409 .
```

Posteriormente, es necesario modificar el archivo `docker-compose.yaml`. En él, cambiamos la propiedad **image: ${AIRFLOW_IMAGE_NAME:-`apache/airflow:2.1.2`}** por **image: {AIRFLOW_IMAGE_NAME:-`extending_airflow:latest202409`}** que corresponde al nombre de la imagen actualizada.

Ejecutar el siguiente comando antes de lanzar el `docker-compose.yml` de Airflow es una buena práctica, especialmente si deseamos configurar correctamente las variables de entorno `AIRFLOW_UID` y `AIRFLOW_GID` para la propiedad `user: "${AIRFLOW_UID:-50000}:${AIRFLOW_GID:-50000}"` indicada en el `docker-compose.yml`. Lanza los siguientes comandos en consola bash:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
echo "AIRFLOW_GID=0" >> .env
```

Te describo el comando a continuación:

- **`AIRFLOW_UID=$(id -u)`**: Este fragmento `$(id -u)` se evalúa y se reemplaza por el UID (User ID) del usuario actual en sistemas Unix/Linux, por tanto, esto establece `AIRFLOW_UID` al ID de usuario actual en tu sistema, lo que es útil para garantizar que los archivos creados por Airflow dentro del contenedor tengan el propietario correcto.
- **`AIRFLOW_GID=0`**: Esto establece `AIRFLOW_GID` al ID de grupo 0, y `0` representa el GID (Group ID) del grupo principal, que es comúnmente el grupo raíz en sistemas Unix/Linux. Esto puede ser útil si deseas que el contenedor tenga permisos de acceso más amplios.
- **`> .env`**: Este comando redirige la salida del `echo` al archivo `.env`, creando el archivo que Docker Compose leerá para establecer las variables de entorno.
- **`>> .env`**: Este operador redirige la salida al archivo `.env`, pero en lugar de sobrescribir el archivo, agrega el texto al final.

¿Por Qué Es Necesario?:

- **Permisos Correctos**: Al establecer `AIRFLOW_UID` y `AIRFLOW_GID`, aseguras que los procesos de Airflow dentro del contenedor tengan los permisos correctos para acceder a los directorios montados en el sistema local (como `./dags`, `./logs`, etc.).
- **Configuración**: Docker Compose usa el archivo `.env` para cargar variables de entorno, permitiendo que tu configuración sea más flexible y reutilizable.

Se creará un archivo **.env**:
```plaintext
/
└── proyecto
    ├── dags
    ├── logs
    ├── plugins
    ├── .env    
    ├── docker-compose.yaml 
    ├── Dockerfile        
    └── requirements.txt   
```

Luego, el siguiente comando que vamos a lanzar será:

```bash
docker compose up airflow-init
```

¿Qué es lo que hace este comando? El contenedor ejecutará el comando definido (en nuestro caso, `version`), y también inicializará la base de datos, realizará la actualización de la base de datos y la creación de un usuario dado que hemos configurado las variables de entorno necesarias en el comando anterior. Una vez que el comando se ejecute y termine, el contenedor `airflow-init` se detendrá, ya que su propósito es inicializar la base de datos una sola vez.

Este comando **se ejecuta una sola vez** para inicializar la base de datos de Airflow y crear al usuario admin. Después de utilizar otros servicios como el `scheduler` y el `webserver`, y detenerlos, y luego volver a iniciarlos, ya no será necesario volver a ejecutar este comando. Sin embargo, si decides borrar todo el entorno, tendrás que ejecutarlo nuevamente para reconfigurar la base de datos y crear el usuario administrativo.

Es importante tener en cuenta que para evitar conflictos entre tu instancia local de PostgreSQL y la de Docker, puedes asignar un puerto diferente al contenedor de PostgreSQL en el archivo `docker-compose.yml`. Por ejemplo, si quieres que el contenedor de PostgreSQL use el puerto **5433** en lugar del **5432**, puedes configurarlo así:

```yaml
    ports:
      - 5433:5432
```

A continuación, podemos establecer la conexión a PostgreSQL que estamos utilizando en Docker a través del gestor de bases de datos DBeaver, como se muestra en la imagen siguiente:

[![p375.png](https://i.postimg.cc/bJZhc5v0/p375.png)](https://postimg.cc/tZGfF2W7)
[![p376.png](https://i.postimg.cc/cCYG68wr/p376.png)](https://postimg.cc/SYyvvRZh)

Ahora queremos arrancar todos los demás servicios:

```bash
# docker compose up -d : se lanza los servicios restantes en el background
docker compose up 
```

Deberíamos tener tres servicios en ejecución: **postgres**, **webserver** y **scheduler**. Para verificar su estado, podemos utilizar el siguiente comando:

```bash
docker ps
```

Con los servicios en funcionamiento, podemos acceder a la interfaz de usuario de Airflow utilizando las siguientes credenciales:

- **Usuario:** airflow
- **Contraseña:** airflow

Puedes ingresar a la UI en el siguiente enlace:

```plaintext
https://localhost:8080
```

Debemos ir a la pestaña **Admin > Connections** y luego agregar una nueva conexión, tal como se muestra en las siguientes imágenes:

[![p373.png](https://i.postimg.cc/28TtzwBV/p373.png)](https://postimg.cc/KRkfftf2)
[![p374.png](https://i.postimg.cc/02ttgyLS/p374.png)](https://postimg.cc/JGk3ZM77) <ancho=575>

Utilizamos la siguiente configuración para la conexión con PostgreSQL:

- **Conn Id:** postgres_docker
- **Conn Type:** Postgres
- **Host:** host.docker.internal
- **Schema:** postgres (es la base de datos que vamos a utilizar, recordar que la base de datos **airflow** es aquella que solo almacena la metadata)
- **Login:** airflow
- **Password:** airflow
- **Port:** 5433

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

[![p378.png](https://i.postimg.cc/65P9hSrN/p378.png)](https://postimg.cc/CzjThcrv)

Y luego podemos revisar las tablas creadas en Postgres por medio de DBeaver:

[![p379.png](https://i.postimg.cc/6QfJF1vq/p379.png)](https://postimg.cc/wtMGm2FY)
[![p380.png](https://i.postimg.cc/fLWnsjcK/p380.png)](https://postimg.cc/NynP7XP9)