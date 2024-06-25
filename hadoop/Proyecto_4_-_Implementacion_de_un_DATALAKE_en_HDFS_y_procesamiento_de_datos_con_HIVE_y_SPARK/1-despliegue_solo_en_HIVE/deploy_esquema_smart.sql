-- 
-- @author Alfonso Perez
-- @email perezlino@gmail.com
-- @copyright Alfonso Perez
--
-- Despliegue del esquema "SMART"
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
-- SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=9999;
SET hive.exec.max.dynamic.partitions.pernode=9999;
SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;
-- SET orc.compression=SNAPPY;
-- SET avro.output.codec=SNAPPY;
-- SET mapreduce.job.queue.name=q_user_main;
-- SET spark.job.queue.name=q_user_main;
-- SET tez.job.queue.name=q_user_main;

--
-- @section Programa
--

-- Borrado de la base de datos
DROP DATABASE IF EXISTS SMART CASCADE;

-- Creación de la base de datos
CREATE DATABASE IF NOT EXISTS SMART 
LOCATION '/user/${hiveconf:PARAM_RAIZ}/${hiveconf:PARAM_PROYECTO}/database/smart';

--
-- Tabla Transaccion_por_Edad
--

-- Creación de la tabla
CREATE TABLE SMART.TRANSACCION_POR_EDAD(
EDAD_PERSONA INT,
CANTIDAD_TRANSACCIONES INT,
SUMA_MONTO_TRANSACCIONES DOUBLE
)
STORED AS PARQUET
LOCATION '/user/proyectos/proyecto3/database/smart/transaccion_por_edad'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Inserción de datos
INSERT OVERWRITE TABLE SMART.TRANSACCION_POR_EDAD
SELECT
  EDAD_PERSONA,
  COUNT(MONTO_TRANSACCION),
  SUM(MONTO_TRANSACCION)
FROM
  UNIVERSAL.TRANSACCION_ENRIQUECIDA
GROUP BY
  EDAD_PERSONA;

-- Impresión de datos
SELECT * FROM SMART.TRANSACCION_POR_EDAD LIMIT 10; 

--
-- Tabla Transaccion_por_Trabajo
--

-- Creación de la tabla
CREATE TABLE SMART.TRANSACCION_POR_TRABAJO(
TRABAJO_PERSONA STRING,
CANTIDAD_TRANSACCIONES INT,
SUMA_MONTO_TRANSACCIONES DOUBLE
)
STORED AS PARQUET
LOCATION '/user/proyectos/proyecto3/database/smart/transaccion_por_trabajo'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Inserción de datos
INSERT OVERWRITE TABLE SMART.TRANSACCION_POR_TRABAJO
SELECT
  TRABAJO_PERSONA,
  COUNT(MONTO_TRANSACCION),
  SUM(MONTO_TRANSACCION)
FROM
  UNIVERSAL.TRANSACCION_ENRIQUECIDA
GROUP BY
  TRABAJO_PERSONA;

-- Impresión de datos
SELECT * FROM SMART.TRANSACCION_POR_TRABAJO LIMIT 10;

--
-- Tabla Transaccion_por_Empresa
--

-- Creación de la tabla
CREATE TABLE SMART.TRANSACCION_POR_EMPRESA(
EMPRESA_TRANSACCION STRING,
CANTIDAD_TRANSACCIONES INT,
SUMA_MONTO_TRANSACCIONES DOUBLE
)
STORED AS PARQUET
LOCATION '/user/proyectos/proyecto3/database/smart/transaccion_por_empresa'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Inserción de datos
INSERT OVERWRITE TABLE SMART.TRANSACCION_POR_EMPRESA
SELECT
  EMPRESA_TRANSACCION,
  COUNT(MONTO_TRANSACCION),
  SUM(MONTO_TRANSACCION)
FROM
  UNIVERSAL.TRANSACCION_ENRIQUECIDA
GROUP BY
  EMPRESA_TRANSACCION;

-- Impresión de datos
SELECT * FROM SMART.TRANSACCION_POR_EMPRESA LIMIT 10;