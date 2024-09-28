import logging
import logging.config
import psycopg2
from psycopg2 import sql

## En el Dockerfile del contenedor "spark-master"
# Instalamos la libreria "psycopg2-binary" que nos permita trabajar con PostgreSQL
# RUN pip install psycopg2-binary

# Cargar el Archivo de Configuración de Logging
logging.config.fileConfig(fname='../util/logging_to_file.conf')

# Obtener el custom Logger desde el archivo de configuracion Logging
logger = logging.getLogger(__name__)

def create_database(db_name):
    """Crea una base de datos en PostgreSQL."""
    try:
        # Conectar al servidor PostgreSQL
        connection = psycopg2.connect(
            dbname='postgres',  # Conectar a la base de datos por defecto
            user='admin',
            password='admin',
            host='postgres',  # Cambia esto si es necesario
            port='5432'
        )
        connection.autocommit = True  # Permite ejecutar comandos sin tener que hacer commit manual

        # Crear un cursor para ejecutar comandos SQL
        cursor = connection.cursor()

        # Verificar si la base de datos ya existe
        cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = {}").format(sql.Identifier(db_name)))
        exists = cursor.fetchone()

        if exists:
            # Si la base de datos existe, eliminarla
            cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(db_name)))
            logging.info(f"Base de datos '{db_name}' eliminada exitosamente.")

        # Crear la base de datos
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        logging.info(f"Base de datos '{db_name}' creada exitosamente.")
        
    except Exception as e:
        logging.error(f"Error al crear la base de datos '{db_name}': {str(e)}")
    
    finally:
        # Cerrar la conexión
        if cursor:
            cursor.close()
        if connection:
            connection.close()