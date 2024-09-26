import logging
import psycopg2
import os
from crear_dataframes import crear_base_df, crear_creditscore_df, crear_exited_age_correlation, crear_exited_salary_correlation
from dotenv import load_dotenv

load_dotenv()
postgres_host = os.environ.get('postgres_host')
postgres_database = os.environ.get('postgres_database')
postgres_user = os.environ.get('postgres_user')
postgres_password = os.environ.get('postgres_password')
postgres_port = os.environ.get('postgres_port')
dest_folder = os.environ.get('dest_folder')


logging.basicConfig(level=logging.INFO, filename='log.log', filemode='a',
                    format='%(asctime)s:%(filename)s:%(funcName)s:%(levelname)s:%(message)s')

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


def crear_nuevas_tablas_en_postgres():
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_creditscore (geography VARCHAR(50), gender VARCHAR(20), avg_credit_score FLOAT, total_exited INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_age_correlation (geography VARCHAR(50), gender VARCHAR(20), exited INTEGER, avg_age FLOAT, avg_salary FLOAT,number_of_exited_or_not INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_salary_correlation  (exited INTEGER, is_greater INTEGER, correlation INTEGER)""")
        logging.info("Se crearon 3 tablas exitosamente en Postgres server")
    except Exception as e:
        logging.error(f'Las tablas no pudieron ser creadas debido a: {e}')


def insertar_creditscore_tabla(df_creditscore):
    query = "INSERT INTO churn_modelling_creditscore (geography, gender, avg_credit_score, total_exited) VALUES (%s,%s,%s,%s)"
    row_count = 0
    for _, row in df_creditscore.iterrows():
        values = (row['geography'],row['gender'],row['avg_credit_score'],row['total_exited'])
        cur.execute(query,values)
        row_count += 1
    
    logging.info(f"{row_count} filas insertadas en la tabla churn_modelling_creditscore")


def insertar_exited_age_correlation_tabla(df_exited_age_correlation):
    query = """INSERT INTO churn_modelling_exited_age_correlation (Geography, Gender, exited, avg_age, avg_salary, number_of_exited_or_not) VALUES (%s,%s,%s,%s,%s,%s)"""
    row_count = 0
    for _, row in df_exited_age_correlation.iterrows():
        values = (row['geography'],row['gender'],row['exited'],row['avg_age'],row['avg_salary'],row['number_of_exited_or_not'])
        cur.execute(query,values)
        row_count += 1
    
    logging.info(f"{row_count} filas insertadas en la tabla churn_modelling_exited_age_correlation")


def insertar_exited_salary_correlation_tabla(df_exited_salary_correlation):
    query = """INSERT INTO churn_modelling_exited_salary_correlation (exited, is_greater, correlation) VALUES (%s,%s,%s)"""
    row_count = 0
    for _, row in df_exited_salary_correlation.iterrows():
        values = (int(row['exited']),int(row['is_greater']),int(row['correlation']))
        cur.execute(query,values)
        row_count += 1

    logging.info(f"{row_count} filas insertadas en la tabla churn_modelling_exited_salary_correlation")


def mover_df_a_postgres_main():
    main_df = crear_base_df(cur)
    df_creditscore = crear_creditscore_df(main_df)
    df_exited_age_correlation = crear_exited_age_correlation(main_df)
    df_exited_salary_correlation = crear_exited_salary_correlation(main_df)

    crear_nuevas_tablas_en_postgres()
    insertar_creditscore_tabla(df_creditscore)
    insertar_exited_age_correlation_tabla(df_exited_age_correlation)
    insertar_exited_salary_correlation_tabla(df_exited_salary_correlation)

    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    mover_df_a_postgres_main()