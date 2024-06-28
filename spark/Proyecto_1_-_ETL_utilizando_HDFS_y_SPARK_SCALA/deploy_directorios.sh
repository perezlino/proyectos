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
## - DATA_EMPRESA.txt
## - DATA_PERSONA.txt
## - DATA_TRANSACCION.txt
##
## al directorio /hadoop-data/proyecto_2/archivos ubicado en el contenedor "namenode"

#Eliminamos la carpeta si existe
echo "Eliminando carpeta raiz..."
hdfs dfs -rm -r -f /user/$PARAM_RAIZ/$PARAM_PROYECTO

#Directorio para almacenar archivos de datos de entrada
echo "Creando directorio para almacenar archivos de entrada..."
hdfs dfs -mkdir -p /user/$PARAM_RAIZ/$PARAM_PROYECTO/input

#Directorio para almacenar archivos de salida
echo "Creando directorio para almacenar archivos de salida..."
hdfs dfs -mkdir -p /user/$PARAM_RAIZ/$PARAM_PROYECTO/output

#Subida de archivos de datos de entrada
echo "Subiendo archivos de entrada..."
hdfs dfs -put \
/hadoop-data/proyecto_2/archivos/DATA_EMPRESA.txt \
/hadoop-data/proyecto_2/archivos/DATA_PERSONA.txt \
/hadoop-data/proyecto_2/archivos/DATA_TRANSACCION.txt \
/user/$PARAM_RAIZ/$PARAM_PROYECTO/input


                                   #  --------------------------------------------
                                   #  EJECUCION DE SCRIPTS DE SOLUCION POR CONSOLA
                                   #  --------------------------------------------
                                    
# En consola se escribe:
                          #   ______________________________________________________________
                          #  |                                                              |         
                          #  |   sh /home/.../deploy_directorios.sh proyectos proyecto3     |
                          #  |______________________________________________________________|

# 1.- Antes de ejecutar un archivo ".sh" debo configurarlo como archivo "UNIX".
# 2.- En NOTEPAD++ ---> Editar ---> Conversion fin de linea ---> Convertir a formato UNIX
# 3.- Luego, lo ejecutamos en MobaXterm.                            