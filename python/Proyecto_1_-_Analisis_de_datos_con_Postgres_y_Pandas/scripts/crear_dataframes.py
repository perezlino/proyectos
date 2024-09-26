"""
Crea un dataframe base a partir de la tabla churn_modelling y crea 3 dataframes separados a partir de él
"""
import psycopg2
import os
import logging
import pandas as pd
import numpy as np
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


def crear_base_df(cur):
    """
    Dataframe base de la tabla churn_modelling
    """
    cur.execute("""SELECT * FROM churn_modelling""")
    rows = cur.fetchall()
    #print(rows)
    # [(1, 15634602, 'Hargrave', 619, 'France', 'Female', 42, 2, 0.0, 1, 1, 1, 101348.88, 1), 
    #  (2, 15647311, 'Hill', 608, 'Spain', 'Female', 41, 1, 83807.86, 1, 0, 1, 112542.58, 0), ..... ]

    # Obtenemos los nombres de las columnas
    # cur.description es una lista de tuplas, donde cada tupla contiene información sobre una columna, como su nombre, tipo de datos, etc.
    # cur.description[0][0] te daría el nombre de la primera columna.
    # cur.description[0][1] podría darte el tipo de dato de la primera columna.
    col_names = [desc[0] for desc in cur.description]
    #print(col_names) ==> ['rownumber', 'customerid', 'surname', 'creditscore', 'geography', 'gender', 'age', 'tenure', 'balance', 'numofproducts', 'hascrcard', 'isactivemember', 'estimatedsalary', 'exited']
    df = pd.DataFrame(rows, columns=col_names)

    df.drop('rownumber', axis=1, inplace=True)
    # Crea 30 numeros de manera aleatoria que se encuentren entre 1 y 10000
    index_to_be_null = np.random.randint(10000, size=30)
    # Los 30 numeros de "index_to_be_null" se utilizaran como 30 indices
    # por tanto, toma 30 registros y modifica de estas 3 columnas su valor
    # Estos valores toman el valor de NaN
    df.loc[index_to_be_null, ['balance','creditscore','geography']] = np.nan
    
    # most_occured_country es una serie ==> <pais> <cantidad>
    # y con index[0] captura el primer pais de esa serie, el pais con mas ocurrencias
    most_occured_country = df['geography'].value_counts().index[0]
    # y todos los valores NaN que tenga la columna "geography" los reemplazara con el valor que se encuentra en la variable "monst_occured_country"
    df['geography'] = df['geography'].fillna(value=most_occured_country)
    
    # la variable "avg_balance" captura el valor del promedio de la columna "balance"
    avg_balance = df['balance'].mean()
    # y todos los valores NaN que tenga la columna "balance" los reemplazara con el valor que se encuentra en la variable "avg_balance"
    df['balance'] = df['balance'].fillna(value=avg_balance)

    # la variable "median_creditscore" captura el valor de la mediana de la columna "creditscore"
    median_creditscore = df['creditscore'].median()
    # y todos los valores NaN que tenga la columna "creditscore" los reemplazara con el valor que se encuentra en la variable "median_creditscore"
    df['creditscore'] = df['creditscore'].fillna(value=median_creditscore)

    return df


def crear_creditscore_df(df):
    df_creditscore = df[['geography', 'gender', 'exited', 'creditscore']].groupby(['geography','gender']).agg({'creditscore':'mean', 'exited':'sum'})
    df_creditscore.rename(columns={'exited':'total_exited', 'creditscore':'avg_credit_score'}, inplace=True)
    df_creditscore.reset_index(inplace=True)

    df_creditscore.sort_values('avg_credit_score', inplace=True)

    return df_creditscore


def crear_exited_age_correlation(df):
    df_exited_age_correlation = df.groupby(['geography', 'gender', 'exited']).agg({
    'age': 'mean',
    'estimatedsalary': 'mean',
    'exited': 'count'
    }).rename(columns={
        'age': 'avg_age',
        'estimatedsalary': 'avg_salary',
        'exited': 'number_of_exited_or_not'
    }).reset_index().sort_values('number_of_exited_or_not')

    return df_exited_age_correlation


def crear_exited_salary_correlation(df):
    df_salary = df[['geography','gender','exited','estimatedsalary']].groupby(['geography','gender']).agg({'estimatedsalary':'mean'}).sort_values('estimatedsalary')
    df_salary.reset_index(inplace=True)

    min_salary = round(df_salary['estimatedsalary'].min(),0)

    df['is_greater'] = df['estimatedsalary'].apply(lambda x: 1 if x>min_salary else 0)

    df_exited_salary_correlation = pd.DataFrame({
    'exited': df['exited'],
    'is_greater': df['estimatedsalary'] > df['estimatedsalary'].min(),
    'correlation': np.where(df['exited'] == (df['estimatedsalary'] > df['estimatedsalary'].min()), 1, 0)
    })

    return df_exited_salary_correlation
