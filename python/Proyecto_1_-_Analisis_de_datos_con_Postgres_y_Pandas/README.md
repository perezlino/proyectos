# Proyecto de Análisis de Datos con PostgreSQL y Pandas

<p align="center">
  <img src="https://i.postimg.cc/MKTVzqDG/p47.png" alt="Descripción de la imagen">
</p>

Este proyecto se compone de tres scripts que realizan una serie de operaciones sobre datos utilizando las siguientes módulos: `psycopg2`, `os`, `logging`, `pandas`, `urllib.request` y `dotenv`.

### Descripción de los Scripts

1. **Descarga y Carga en PostgreSQL**: 
   En el primer script, se descarga un archivo CSV desde una URL especificada. Utilizando `psycopg2`, se establece una conexión con un servidor PostgreSQL y se crea una nueva tabla para almacenar los datos. Posteriormente, se lee el archivo CSV como un DataFrame de `pandas` y se insertan los registros en la tabla creada.

2. **Creación de DataFrames**: 
   El segundo script genera un DataFrame base a partir de la tabla `churn_modelling` en PostgreSQL. A partir de este DataFrame base, se crean tres DataFrames separados que permiten un análisis más granular de los datos.

3. **Carga de DataFrames en PostgreSQL**: 
   En el tercer script, los tres DataFrames generados se cargan nuevamente en el servidor PostgreSQL, asegurando que los datos estén disponibles para futuros análisis.

### Requisitos

Para ejecutar este proyecto, asegúrate de tener instaladas las siguientes librerias:

- `psycopg2`
- `pandas`
- `python-dotenv`

Además, utiliza un archivo `.env` para gestionar tus credenciales y configuraciones de entorno de manera segura.

### Configuración de Logging

Se ha implementado un sistema de logging utilizando el módulo `logging` para registrar las actividades de cada script, lo que facilita la depuración y el seguimiento del proceso de carga de datos.

### Verificar resultados

Por medio de linea de comandos podemos verificar que se hayan creado las 4 tablas que creamos en este proyecto. 
Comenzamos arrancando Postgres y usando la base de datos `postgres`:
```bash
psql -U postgres --password --dbname postgres
```
Luego, visualizamos las tablas que se han creado:
```bash
postgres=# \dt
                         Listado de relaciones
 Esquema |                  Nombre                   | Tipo  |  Due±o
---------+-------------------------------------------+-------+----------
 public  | churn_modelling                           | tabla | postgres
 public  | churn_modelling_creditscore               | tabla | postgres
 public  | churn_modelling_exited_age_correlation    | tabla | postgres
 public  | churn_modelling_exited_salary_correlation | tabla | postgres
(4 filas)
```
Y finalmente, consultamos las tablas para verificar que efectivamente tengan datos:
```bash
postgres=# SELECT * FROM churn_modelling LIMIT 10;
postgres=# SELECT * FROM churn_modelling_creditscore LIMIT 10;
postgres=# SELECT * FROM churn_modelling_exited_age_correlation LIMIT 10;
postgres=# SELECT * FROM churn_modelling_exited_salary_correlation LIMIT 10;
``` 