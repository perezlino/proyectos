"""
Descarga el archivo csv de la URL. Crea una nueva tabla en el servidor Postgres.
Lee el archivo como un dataframe e inserta cada registro en la tabla Postgres. 
"""
import psycopg2
import os
import logging
import pandas as pd
import urllib.request
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, filename='log.log', filemode='a',
                    format='%(asctime)s:%(filename)s:%(funcName)s:%(levelname)s:%(message)s')

load_dotenv()
postgres_host = os.environ.get('postgres_host')
postgres_database = os.environ.get('postgres_database')
postgres_user = os.environ.get('postgres_user')
postgres_password = os.environ.get('postgres_password')
postgres_port = os.environ.get('postgres_port')
dest_folder = os.environ.get('dest_folder')

url = "https://raw.githubusercontent.com/dogukannulu/datasets/master/Churn_Modelling.csv"
destination_path = f'{dest_folder}/churn_modelling.csv' 

try:
    conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password,
        port=postgres_port
    )
    cur = conn.cursor()
    logging.info('La conexión con el servidor Postgres se ha realizado correctamente')
except Exception as e:
    logging.error("No se ha podido crear la conexión Postgres")


def descargar_archivo_desde_url(url: str, dest_folder: str):
    """
    Descargar un archivo de una URL específica y descargarlo en la dirección local
    """
    if not os.path.exists(str(dest_folder)):
        os.makedirs(str(dest_folder))  # crear carpeta si no existe

    try:
        urllib.request.urlretrieve(url, destination_path)
        logging.info('archivo csv descargado correctamente en el directorio de trabajo')
    except Exception as e:
        logging.error(f'Error al descargar el archivo csv debido a: {e}')


def crear_tabla_postgres():
    """
    Crear la tabla Postgres con el schema deseado
    """
    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS churn_modelling 
        (
            RowNumber INTEGER PRIMARY KEY, 
            CustomerId INTEGER, 
            Surname VARCHAR(50), 
            CreditScore INTEGER, 
            Geography VARCHAR(50), 
            Gender VARCHAR(20), 
            Age INTEGER, 
            Tenure INTEGER, 
            Balance FLOAT, 
            NumOfProducts INTEGER, 
            HasCrCard INTEGER, 
            IsActiveMember INTEGER, 
            EstimatedSalary FLOAT, 
            Exited INTEGER
        )
        """)
        
        logging.info('Nueva tabla churn_modelling creada con éxito en el servidor postgres')
    except:
        logging.warning('Comprobar si existe la tabla churn_modelling')


def mover_csv_a_postgres():
    """
    Escribir en la tabla Postgres el archivo CSV
    """
    ################################# Forma 1 - Utilizando COPY | FROM #################################

    # query = f'''COPY churn_modelling (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, 
    #         Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited)
    # FROM '{destination_path}'
    # DELIMITER ','
    # CSV HEADER;'''
    # cur.execute(query)

    ####################################################################################################
    ################################## Forma 2 - Copiando el Dataframe #################################
    df = pd.read_csv(f'{dest_folder}/churn_modelling.csv')
    inserted_row_count = 0

    for _, row in df.iterrows():
        count_query = f"""SELECT COUNT(*) FROM churn_modelling WHERE RowNumber = {row['RowNumber']}""" # ==> Iria iterando ==> SELECT COUNT(*) FROM churn_modelling WHERE RowNumber = 1
        cur.execute(count_query)                                                                       #                       SELECT COUNT(*) FROM churn_modelling WHERE RowNumber = 2
        result = cur.fetchone()                                                                         #                      SELECT COUNT(*) FROM churn_modelling WHERE RowNumber = 3
        #print(result) ==> En cada interación me devolvería ==> (0,) ==> porque por el COUNT indicado para dicha iteración dara 0, porque dicha fila no existe aun en la tabla    

        if result[0] == 0:
            inserted_row_count += 1
            cur.execute("""INSERT INTO churn_modelling (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, 
            Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)""", 
            (int(row['RowNumber']), int(row['CustomerId']), str(row['Surname']), int(row['CreditScore']), str(row['Geography']), 
             str(row['Gender']), int(row['Age']), int(row['Tenure']), float(row['Balance']), int(row['NumOfProducts']), int(row['HasCrCard']),
             int(row['IsActiveMember']), float(row['EstimatedSalary']), int(row['Exited'])))    

    # Recordar que "row" es cada registro del Dataframe entregado por iterrows()
    # row[0] == row['RowNumber']
    # row[1] == row['CustomerId'] y asi sucesivamente

    logging.info(f' {inserted_row_count} csv insertado correctamente en la tabla churn_modelling')
    ####################################################################################################
    
def mover_csv_a_postgres_main():
    descargar_archivo_desde_url(url, dest_folder)
    crear_tabla_postgres()
    mover_csv_a_postgres()
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    mover_csv_a_postgres_main()