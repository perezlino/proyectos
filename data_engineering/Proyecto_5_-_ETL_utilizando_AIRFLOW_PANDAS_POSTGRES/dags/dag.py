from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
from pandas import json_normalize
from datetime import timedelta, datetime

OUTPUT = '/opt/airflow/dags/tmp/processed_user.csv'

DEFAULT_ARGS = {
    'owner': 'Alfonso Perez',
    'retries' : 2,
    'retry_delay': timedelta (minutes=0.5)
}

# Se añade un parámetro 'ti' por task instance. No voy a bucear en los detalles aquí de 
# por que esto es necesario, pero básicamente necesitamos ese parámetro para extraer los 
# datos (pull the data) que han sido descargados por la task 'extract_user'.
def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user")
    user = user['results'][0]
    processed_user = json_normalize({
        'firstname':user['name']['first'],
        'lastname':user['name']['last'],
        'country':user['location']['country'],
        'username':user['login']['username'],
        'password':user['login']['password'],
        'email':user['email']
    })
    processed_user.to_csv(OUTPUT, index=None, header=False)

def _store_user():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql = "COPY users FROM stdin WITH DELIMITER as ','",
        filename = OUTPUT
    )

with DAG(
    'user_processing', 
    description = 'Ejecucion Data Pipeline Procesamiento de Usuarios',    
    start_date=datetime(2024,10,2), 
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily', 
    catchup=False,
    tags = ['data engineering']

) as dag:
    
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgres',
        sql = '''
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
             '''
        )

    # Sensor que comprobará si la API está disponible o no.
    is_api_available = HttpSensor(
        task_id = 'is_api_available',
        http_conn_id = 'user_api',
        endpoint = 'api/',
        response_check=lambda response: response.status_code == 200,
        poke_interval=10,
        timeout=600
    )  

    extract_user = SimpleHttpOperator(
        task_id = 'extract_user',
        http_conn_id = 'user_api',
        endpoint = 'api/',
        method = 'GET',
        # Trae la información en formato String (response.text) y luego la transformamos a un
        # diccionario python (json.loads(response.text)) y queda almacenado en la variable 'response'
        response_filter = lambda response: json.loads(response.text),
        # Luego, por último, pero no menos importante, queremos registrar (log) la respuesta para que 
        # usted sea capaz de ver la respuesta que se obtiene en los logs en la interfaz de usuario.
        log_response = True
    )

    process_user = PythonOperator(
        task_id = 'process_user',
        python_callable=_process_user
    )

    # Implementamos la tarea "store_user" que utiliza el hook de Postgres para copiar los usuarios 
    # del archivo .csv en la tabla "users" que creaste en la primera tarea "create_table" y ten en 
    # cuenta que el método "copy_expert" no existe desde el operador PostgreSQL. Tienes que usar el 
    # PostgresHook. Se recomienda revisar el hook para ver qué métodos se pueden usar.
    store_user = PythonOperator(
        task_id='store_user',
        python_callable=_store_user
    )

    create_table >> is_api_available >> extract_user >> process_user >> store_user