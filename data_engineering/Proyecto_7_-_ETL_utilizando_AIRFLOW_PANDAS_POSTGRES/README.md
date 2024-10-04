# Data Pipeline para el procesamiento de datos de usuarios a través de una API utilizando Airflow, Pandas y PostgreSQL

[![p381.png](https://i.postimg.cc/QMvZ6NJg/p381.png)](https://postimg.cc/d7Rf3YXD)

Este proyecto utiliza Apache Airflow para procesar datos de usuarios a través de una API y almacenarlos en una base de datos PostgreSQL. El pipeline incluye la creación de una tabla, la verificación de la disponibilidad de la API, la extracción de datos, el procesamiento de esos datos y, finalmente, el almacenamiento en la base de datos.

## Descripción del DAG

El DAG `user_processing` realiza las siguientes tareas:

1. **Crear Tabla**: Crea una tabla llamada `users` en PostgreSQL si no existe ya. Esta tabla almacena los detalles de los usuarios extraídos.

2. **Verificar Disponibilidad de la API**: Utiliza el `HttpSensor` para comprobar si la API de usuarios está disponible antes de continuar con el flujo de trabajo.

3. **Extraer Usuario**: Utiliza el `SimpleHttpOperator` para realizar una solicitud `GET` a la API y obtener información de usuarios en formato JSON.

4. **Procesar Usuario**: Implementa una función Python que transforma los datos extraídos en un formato adecuado y los guarda en un archivo CSV.

5. **Almacenar Usuario**: Utiliza un `PostgresHook` para copiar los datos procesados desde el archivo CSV a la tabla `users` en PostgreSQL.

## Configuración

1. **Conexiones**: Asegúrate de tener configuradas las conexiones necesarias en Airflow:
   - `postgres`: Conexión a la base de datos PostgreSQL.
   - `user_api`: Conexión a la API de usuarios.

2. **Directorio de Salida**: Verifica que el directorio `/opt/airflow/dags/tmp/` exista para almacenar el archivo CSV procesado.

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

También crearemos un subdirectorio dentro del directorio `dags`:

```bash
mkdir -p ./dags/tmp
```

Debemos crear el script de Python **dag.py** en el directorio **dags**, donde definiremos nuestro DAG.

```plaintext
/
└── dags
    ├── tmp
    └── dag.py
```

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
    └── docker-compose.yaml   
```

Luego, el siguiente comando que vamos a lanzar será:

```bash
# docker compose up airflow-init -d : se ejecuta en segundo plano
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
# docker compose up -d : se lanza los servicios restantes en segundo plano
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

Debemos ir a la pestaña **Admin > Connections** y luego agregar dos conexiones:

[![p373.png](https://i.postimg.cc/28TtzwBV/p373.png)](https://postimg.cc/KRkfftf2)
[![p374.png](https://i.postimg.cc/02ttgyLS/p374.png)](https://postimg.cc/JGk3ZM77) <ancho=575>

Configuramos la conexión con **PostgreSQL** de la siguiente manera:

- **Conn Id:** postgres
- **Conn Type:** Postgres
- **Host:** host.docker.internal
- **Schema:** postgres (esta es la base de datos que utilizaremos; ten en cuenta que la base de datos **airflow** se utiliza exclusivamente para almacenar la metadata)
- **Login:** airflow
- **Password:** airflow
- **Port:** 5433

Además, debemos configurar otra conexión que se utilizará con el **HttpSensor**, con los siguientes detalles:

- **Conn Id:** user_api
- **Conn Type:** HTTP
- **Host:** https://randomuser.me/

[![p382.png](https://i.postimg.cc/GtqM5q10/p382.png)](https://postimg.cc/FkkgfbRG)

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

[![p383.png](https://i.postimg.cc/pVZ1rX50/p383.png)](https://postimg.cc/z3ykPNjW)

Y luego podemos revisar las tablas creadas en Postgres por medio de DBeaver:

[![p384.png](https://i.postimg.cc/hPtZdW9K/p384.png)](https://postimg.cc/w3nQCrfr)