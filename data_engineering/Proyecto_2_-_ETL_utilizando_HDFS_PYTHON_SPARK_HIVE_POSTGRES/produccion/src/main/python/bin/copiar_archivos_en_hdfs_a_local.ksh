#################################################################
# Autor: Alfonso Pérez Lino                                     #
# Fecha:                                                        #
# Objetivo: Borrar los archivos en las rutas locales si existen #
#           y luego copiar archivos desde HDFS hacia Entorno    #
#           local                                               #
#################################################################

#Declara una variable para contener el nombre del script unix.
JOBNAME="copiar_archivos_en_hdfs_a_local.ksh"

#Declara una variable para mantener la fecha actual
date=$(date '+%Y-%m-%d_%H:%M:%S')

#Define un Archivo Log donde se generarán los logs
LOGFILE="/produccion/src/main/python/logs/${JOBNAME}_${date}.log"

######################################################################
### COMENTARIOS: A partir de este punto, toda la salida estándar y el 
### error estándar se registrarán en el archivo log.
######################################################################
{  # <--- Inicio del archivo log.
echo "${JOBNAME} Se ha iniciado...: $(date)"
LOCAL_OUTPUT_PATH="/produccion/src/main/python/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/presc

HDFS_OUTPUT_PATH=/proyectos/PrescPipeline/output
HDFS_CITY_DIR=${HDFS_OUTPUT_PATH}/dimension_city
HDFS_FACT_DIR=${HDFS_OUTPUT_PATH}/presc

### Borrar los archivos en las rutas locales si existen
rm -f ${LOCAL_CITY_DIR}/*
rm -f ${LOCAL_FACT_DIR}/*

### Copiar el archivo 'dimension_city' y 'fact' de HDFS a Local
hdfs dfs -get -f ${HDFS_CITY_DIR}/* ${LOCAL_CITY_DIR}/
hdfs dfs -get -f ${HDFS_FACT_DIR}/* ${LOCAL_FACT_DIR}/
echo "${JOBNAME} a finalizado...: $(date)"

} > ${LOGFILE} 2>&1  # <--- Fin del programa y fin del log.
