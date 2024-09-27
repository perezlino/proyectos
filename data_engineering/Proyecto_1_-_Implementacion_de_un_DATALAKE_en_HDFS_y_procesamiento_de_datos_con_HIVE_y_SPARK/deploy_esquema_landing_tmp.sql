-- 
-- @author Alfonso Perez
-- @email perezlino@gmail.com
-- @copyright Alfonso Perez
--
-- Despliegue del esquema "LANDING"
-- 

-- 
-- @section Tuning
-- 

-- SET hive.execution.engine=mr;
-- SET mapreduce.job.maps=8;
-- SET mapreduce.input.fileinputformat.split.maxsize=128000000;
-- SET mapreduce.input.fileinputformat.split.minsize=128000000;
-- SET mapreduce.map.cpu.vcores=2;
-- SET mapreduce.map.memory.mb=128;
-- SET mapreduce.job.reduces=8;
-- SET mapreduce.reduce.cpu.vcores=2;
-- SET mapreduce.reduce.memory.mb=128;
-- SET hive.exec.dynamic.partition=true;     # <------- Activado en "true" por defecto desde Hive 0.14.0
SET hive.exec.dynamic.partition.mode=nonstrict;
-- SET hive.exec.max.dynamic.partitions=9999;
-- SET hive.exec.max.dynamic.partitions.pernode=9999;
SET hive.exec.compress.output=true;
-- SET parquet.compression=SNAPPY;
-- SET orc.compression=SNAPPY;
SET avro.output.codec=SNAPPY;
-- SET mapreduce.job.queue.name=q_user_main;
-- SET spark.job.queue.name=q_user_main;
-- SET tez.job.queue.name=q_user_main;

--
-- @section Programa
--

-- Borrado de la base de datos
DROP DATABASE IF EXISTS LANDING_TMP CASCADE;

-- Creación de la base de datos:
-- en LOCATION podemos indicar solo la ruta: "/user/..." o anteponer el "hdfs:///user/..."
CREATE DATABASE IF NOT EXISTS LANDING_TMP 
LOCATION 'hdfs:///user/${hiveconf:PARAM_RAIZ}/${hiveconf:PARAM_PROYECTO}/database/landing_tmp';

--
-- Tabla Persona
--

-- Creación de la tabla
-- en LOCATION podemos indicar solo la ruta: "/user/..." o anteponer el "hdfs:///user/..."
CREATE TABLE LANDING_TMP.PERSONA(
ID STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO STRING,
EDAD STRING,
SALARIO STRING,
ID_EMPRESA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/${hiveconf:PARAM_RAIZ}/${hiveconf:PARAM_PROYECTO}/database/landing_tmp/persona';

-- Subida de datos
LOAD DATA INPATH '/user/proyectos/datalake/archivos/persona.data' INTO TABLE LANDING_TMP.PERSONA;

-- Impresión de datos
SELECT * FROM LANDING_TMP.PERSONA LIMIT 10;

--
-- Tabla Empresa
--

-- Creación de tabla
-- en LOCATION podemos indicar solo la ruta: "/user/..." o anteponer el "hdfs:///user/..."
CREATE TABLE LANDING_TMP.EMPRESA(
ID STRING,
NOMBRE STRING
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/${hiveconf:PARAM_RAIZ}/${hiveconf:PARAM_PROYECTO}/database/landing_tmp/empresa';

-- Subida de datos
LOAD DATA INPATH '/user/proyectos/datalake/archivos/empresa.data' INTO TABLE LANDING_TMP.EMPRESA; 

-- Impresión de datos
SELECT * FROM LANDING_TMP.EMPRESA LIMIT 10;

--
-- Tabla Transaccion
--

-- Creación de tabla
-- en LOCATION podemos indicar solo la ruta: "/user/..." o anteponer el "hdfs:///user/..."
CREATE TABLE LANDING_TMP.TRANSACCION(
ID_PERSONA STRING,
ID_EMPRESA STRING,
MONTO STRING,
FECHA STRING  
)                                                   
ROW FORMAT DELIMITED                               
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/${hiveconf:PARAM_RAIZ}/${hiveconf:PARAM_PROYECTO}/database/landing_tmp/transaccion';

-- Subida de datos
LOAD DATA INPATH '/user/proyectos/datalake/archivos/transacciones.data' INTO TABLE LANDING_TMP.TRANSACCION;

-- Impresión de datos
SELECT * FROM LANDING_TMP.TRANSACCION LIMIT 10;


                                   --    --------------------------------------------
                                   --    EJECUCION DE SCRIPTS DE SOLUCION POR CONSOLA
                                   --    --------------------------------------------

-- Utilizar este comando para ejecutar script sql en mi estorno de Hadoop personal                                      
--      ___________________________________________________________________________________________________________________________________________
--     |                                                                                                                                           |         
--     |   beeline -u jdbc:hive2://localhost:10000 -f /home/main/proceso_1.sql --hiveconf "PARAM_RAIZ=XXXX" --hiveconf "PARAM_PROYECTO=XXXX"       |
--     |___________________________________________________________________________________________________________________________________________|