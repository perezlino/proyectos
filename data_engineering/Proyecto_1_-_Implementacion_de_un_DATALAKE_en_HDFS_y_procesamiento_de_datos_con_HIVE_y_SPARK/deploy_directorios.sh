#!/bin/bash

##
## @author Alfonso Perez
## @email perezlino@gmail.com
## @copyright Alfonso Perez
##
## Crea la estructura de carpetas
## 

## 
## @section Parámetros
## 

PARAM_RAIZ=$1         #<------ Parametro que se va a recibir en la posicion 1 en la consola
PARAM_PROYECTO=$2     #<------ Parametro que se va a recibir en la posicion 2 en la consola

##
## @section Programa
##

## Antes de ejecutar este archivo mover archivos:
## - persona.avsc
## - empresa.avsc
## - transaccion.avsc
##
## al directorio /hadoop-data/archivos ubicado en el contenedor "namenode"

#Eliminamos la carpeta si existe
echo "Eliminando carpeta raiz..."
hdfs dfs -rm -r -f /user/$PARAM_RAIZ/$PARAM_PROYECTO

#Directorio para almacenar archivos de datos
hdfs dfs -mkdir -p /user/$PARAM_RAIZ/$PARAM_PROYECTO/archivos

#Estructura de carpetas para "landing_tmp"
echo "Creando la estructura de carpetas para landing_tmp..."
hdfs dfs -mkdir -p \
/user/$PARAM_RAIZ/$PARAM_PROYECTO \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/database/landing_tmp/persona \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/database/landing_tmp/empresa \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/database/landing_tmp/transaccion 

#Estructura de carpetas para "landing"
echo "Creando la estructura de carpetas para landing..."
hdfs dfs -mkdir -p \
/user/$PARAM_RAIZ/$PARAM_PROYECTO \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/database/landing/persona \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/database/landing/empresa \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/database/landing/transaccion

hdfs dfs -mkdir -p /user/$PARAM_RAIZ/$PARAM_PROYECTO/schema/landing


#Estructura de carpetas para "universal"
echo "Creando la estructura de carpetas para universal..."
hdfs dfs -mkdir -p \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/database/universal/persona \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/database/universal/empresa \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/database/universal/transaccion \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/database/universal/transaccion_enriquecida

#Estructura de carpetas para "smart"
echo "Creando la estructura de carpetas para smart..."
hdfs dfs -mkdir -p \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/database/smart/transaccion_por_edad \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/database/smart/transaccion_por_trabajo \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/database/smart/transaccion_por_empresa 

#Subida de archivos de datos
echo "Subiendo archivos de datos..."
hdfs dfs -put \
/hadoop-data/archivos/persona.data \
/hadoop-data/archivos/empresa.data \
/hadoop-data/archivos/transacciones.data \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/archivos

#Subida de archivos de "schema"
echo "Subiendo archivos de schema..."
hdfs dfs -put \
/hadoop-data/schemas/persona.avsc \
/hadoop-data/schemas/empresa.avsc \
/hadoop-data/schemas/transaccion.avsc \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/schema/landing


                                   #  --------------------------------------------
                                   #  EJECUCION DE SCRIPTS DE SOLUCION POR CONSOLA
                                   #  --------------------------------------------
                                    
# En consola se escribe:
                          #   ______________________________________________________________
                          #  |                                                              |         
                          #  |   sh /home/.../deploy_directorios.sh proyectos datalake      |
                          #  |______________________________________________________________|

# 1.- Antes de ejecutar un archivo ".sh" debo configurarlo como archivo "UNIX".
# 2.- En NOTEPAD++ ---> Editar ---> Conversion fin de linea ---> Convertir a formato UNIX
# 3.- Luego, lo ejecutamos en MobaXterm.                            