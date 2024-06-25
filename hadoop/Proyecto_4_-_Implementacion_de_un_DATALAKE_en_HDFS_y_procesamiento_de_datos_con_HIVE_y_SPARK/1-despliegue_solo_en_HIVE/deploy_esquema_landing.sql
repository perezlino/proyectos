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
-- SET hive.exec.dynamic.partition=true;    # <------- Activado en "true" por defecto desde Hive 0.14.0
SET hive.exec.dynamic.partition.mode=nonstrict;
-- SET hive.exec.max.dynamic.partitions=9999;
-- SET hive.exec.max.dynamic.partitions.pernode=9999;
SET hive.exec.compress.output=true;
-- SET parquet.compression=SNAPPY;
-- SET orc.compression=SNAPPY;
SET avro.output.codec=snappy;
-- SET mapreduce.job.queue.name=q_user_main;
-- SET spark.job.queue.name=q_user_main;
-- SET tez.job.queue.name=q_user_main;

--
-- @section Programa
--

-- Borrado de la base de datos
DROP DATABASE IF EXISTS LANDING CASCADE;

-- Creación de la base de datos
-- en LOCATION podemos indicar solo la ruta: "/user/..." o anteponer el "hdfs:///user/..."
CREATE DATABASE IF NOT EXISTS LANDING 
LOCATION '/user/${hiveconf:PARAM_RAIZ}/${hiveconf:PARAM_PROYECTO}/database/landing';

--
-- Tabla Persona
--

-- Creación de la tabla
-- en LOCATION podemos indicar solo la ruta: "/user/..." o anteponer el "hdfs:///user/..."
CREATE TABLE LANDING.PERSONA
STORED AS AVRO
LOCATION '/user/${hiveconf:PARAM_RAIZ}/${hiveconf:PARAM_PROYECTO}/database/landing/persona'
TBLPROPERTIES (
'avro.schema.url'='hdfs:///user/${hiveconf:PARAM_RAIZ}/${hiveconf:PARAM_PROYECTO}/schema/landing/persona.avsc',
'avro.output.codec'='snappy'
);

-- Inserción de datos
INSERT OVERWRITE TABLE LANDING.PERSONA SELECT * FROM LANDING_TMP.PERSONA;

-- Impresión de datos
SELECT * FROM LANDING.PERSONA LIMIT 10;  

--
-- Tabla Empresa
--

-- Creación de tabla
-- en LOCATION podemos indicar solo la ruta: "/user/..." o anteponer el "hdfs:///user/..."
CREATE TABLE LANDING.EMPRESA
STORED AS AVRO
LOCATION '/user/${hiveconf:PARAM_RAIZ}/${hiveconf:PARAM_PROYECTO}/database/landing/empresa'
TBLPROPERTIES (
'avro.schema.url'='hdfs:///user/${hiveconf:PARAM_RAIZ}/${hiveconf:PARAM_PROYECTO}/schema/landing/empresa.avsc',
'avro.output.codec'='snappy'
);

-- Inserción de datos
INSERT OVERWRITE TABLE LANDING.EMPRESA SELECT * FROM LANDING_TMP.EMPRESA;

-- Impresión de datos
SELECT * FROM LANDING.EMPRESA LIMIT 10; 

--
-- Tabla Transaccion
--

-- Creación de tabla
-- en LOCATION podemos indicar solo la ruta: "/user/..." o anteponer el "hdfs:///user/..."
CREATE TABLE LANDING.TRANSACCION
PARTITIONED BY (FECHA STRING)
STORED AS AVRO
LOCATION '/user/${hiveconf:PARAM_RAIZ}/${hiveconf:PARAM_PROYECTO}/database/landing/transaccion'
TBLPROPERTIES (
'avro.schema.url'='hdfs:///user/${hiveconf:PARAM_RAIZ}/${hiveconf:PARAM_PROYECTO}/schema/landing/transaccion.avsc',
'avro.output.codec'='snappy'
);

-- Inserción de datos
INSERT OVERWRITE TABLE LANDING.TRANSACCION
PARTITION(FECHA)
SELECT * FROM  LANDING_TMP.TRANSACCION;

-- Impresión de datos
SELECT * FROM LANDING.TRANSACCION LIMIT 10;