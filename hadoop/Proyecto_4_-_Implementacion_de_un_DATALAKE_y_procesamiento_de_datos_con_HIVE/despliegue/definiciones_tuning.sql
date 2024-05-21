/**
 * @section Tuning
 */

-- El tuneo consiste en encontrar la cantidad de recursos computacionales necesaria para que un proceso se ejecute
-- Los recursos computacionales que debemos tunear son las CPUs y la memoria RAM
-- Las CPUs son las que ejecutan los procesamientos, mientras mas CPUs, más paralelo el proceso
-- La RAM permite que el archivo pueda ser volcado de disco duro a memoria, si separamos poca RAM, el archivo no entrará
-- Para tunear los procesos en HIVE necesitamos definir al menos estos cuatro parámetros

/**
 * @section Número de mappers
 */

-- ¿Cuántos mappers elegir?
-- El número de mappers es lo que paraleliza la ejecución del algoritmo (instancias = containers = mappers)
-- Cada mapper recibe una porción del archivo que se procesa
-- Para elegir el número de mappers debemos ver dos cosas:
-- 
-- - El tamaño de la tabla que se procesa
-- - El tamaño de bloque de clúster
-- 
-- El número de mappers inicial recomendado será (Tamaño de tabla / tamaño de bloque)
-- 
-- Por ejemplo, si fuesemos a hacer un "count" con una tabla con las siguientes características:
-- 
-- Tamaño de la tabla: 1024 MB
-- Tamaño del bloque de clúster: 128 MB
--
-- Podríamos colocar como número de mappers 1024/128 = 8
-- De esa manera crearíamos 8 mappers, donde cada uno recibiría un pedazo del archivo
-- Entonces, ¿es 8 el número de mappers ideal para procesar esta tabla?, la respuesta es depende
-- "8" es el número de mappers inicial con el que trabajaremos
-- Por medio de prueba y error iremos aumentando o disminuyendo este número para encontrar uno con el cual nuestro tiempo de procesamiento sea el esperado
-- Generalmente los aumentos o disminuciones se hacen duplicando el número actual de mappers.

-- Parámetro para colocar el número de mappers
SET mapreduce.job.maps=8;

-- También es posible indicarle el número de mappers por medio de cortar el archivo
-- Por ejemplo, si nuestro archivo pesa 1024 MB y queremos tener 8 mappers, necesitaremos cortes de 128 MB
-- Indicamos que queremos cortes de 128 MB así (el valor está en bytes):
SET mapreduce.input.fileinputformat.split.maxsize = 128000000;
SET mapreduce.input.fileinputformat.split.minsize = 128000000;

-- Memoria RAM
SET mapreduce.map.memory.mb=128;

-- Recordemos que un mapper es una unidad de procesamiento
-- Cada mappers que usemos le quitará vcpus al clúster
-- ¿Cuántas vcpus le quita al clúster?, nosotros lo definimos
-- Generalmente, cada mapper debe tener asignado 1, 2 o 4 vcpus
-- Es un estándar trabajar con al menos 2 vcpus para cada mapper
-- Lo indicamos con el siguiente parámetro
SET mapreduce.map.cpu.vcores=2;

/**
 * @section Número de reducers
 */

-- Los reducers son los que juntan los resultados intermedios obtenidos por los mappers
-- Cada reducer recibe una parte de los resultados intermedios procesados
-- ¿Cuántos reducers elegir?, para esto debemos de saber:
-- Colocaremos el mismo número de mappers
SET mapreduce.job.reduces=8;

-- Recordemos que un reducer es una unidad de procesamiento
-- Cada reducer que usemos le quitará vcpus al clúster
-- ¿Cuántas vcpus le quita al clúster?, nosotros lo definimos
-- Generalmente, cada reducer debe tener asignado 1, 2 o 4 vcpus
-- Es un estándar trabajar con al menos 2 vcpus para cada reducer
-- Lo indicamos con el siguiente parámetro
SET mapreduce.reducer.cpu.vcores=2;

/**
 * @section Memoria para los mappers
 */

-- La memoria RAM es el espacio de memoria en donde se carga el archivo que se procesará
-- Si ponemos poca memoria, el archivo no podrá entrar y se tendrá que usar memoria virtual (disco duro), lo cual hará el proceso más lento
-- Debemos indicarle la cantidad de memoria RAM que necesitaremos para cada mapper
-- Por ejemplo, imaginemos que nuestro archivo pesa 1024 MB y hemos decidido tener 8 mappers
-- Eso quiere decir que cada mapper recibirá 1024 MB / 8 = 128 MB
-- Por lo tanto cada mapper deberá tener una memoria asignada de 128 MB, lo indicamos así:
SET mapreduce.map.memory.mb=128;

/**
 * @section Memoria para los reducers
 */

-- Colocaremos el mismo que el mapper
SET mapreduce.reduce.memory.mb=128;

/**
 * @section Procesamiento de datos tuneado
 */

-- Activamos el particionamiento dinámico
SET hive.exec.dynamic.partition=true; 
SET hive.exec.dynamic.partition.mode=nonstrict;




-- Tuning de proceso
-- Averiguaremos los tamaños de tabla (hdfs dfs -du -s -s /ruta/de/mi/tabla)
-- Tamaño de tabla "LANDING.TRANSACCION": 1024 MB
-- Tamaño de tabla "LANDING.PERSONA": 100 MB
-- Tamaño de tabla "LANDING.EMPRESA": 24 MB
-- TAMAÑO MAYOR: 1024 MB                                                                                          Ejemplo de tuneo: Digamos que el proceso con 8 containers
-- Aplicaremos el tuning que hemos puesto de ejemplo                                                               ___________________________________  se demora 2 horas.
SET mapreduce.job.maps=8;                                        <----- n° containers mappers                     | # containers | CPU | RAM | tiempo | Y nos piden hacerlo             
SET mapreduce.input.fileinputformat.split.maxsize = 128000000;   <----- Colocamos el n° de RAM en bytes           |--------------|-----|-----|--------| en 1 hora. Debemos 
SET mapreduce.input.fileinputformat.split.minsize = 128000000;                                                    |      8       |  2  | 128 |   2 h  | aumentar el numero de        
SET mapreduce.map.cpu.vcores=2;                                  <----- n° de cpu mappers                         |      16      |  2  |  64 |   1 h  | containers. Y modificamos          
SET mapreduce.map.memory.mb=128;                                 <----- memoria ram mappers                       |______________|_____|_____|________| con estos nuevos numeros 
SET mapreduce.job.reduces=8;         <----- n° containers reducers (coloco el mismo valor que el de mappers)                                            los encabezados del tunning.
SET mapreduce.reduce.cpu.vcores=2;   <----- n° de cpu mappers (coloco el mismo valor que el de mappers) 
SET mapreduce.reduce.memory.mb=128;  <----- memoria ram mappers (coloco el mismo valor que el de mappers) 

-- Activamos la compresión y el formato SNAPPY
SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;

-- Activamos el particionamiento dinámico
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;