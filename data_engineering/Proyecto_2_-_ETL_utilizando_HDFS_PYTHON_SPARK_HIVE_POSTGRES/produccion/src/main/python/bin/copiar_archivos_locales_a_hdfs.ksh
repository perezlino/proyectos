#################################################################
# Autor: Alfonso Pérez Lino                                     #
# Fecha:                                                        #
# Objetivo: Copiar archivos desde Entorno local hacia HDFS      #
#################################################################

# Declara una variable para contener el nombre del script unix.
JOBNAME="copiar_archivos_locales_a_hdfs.ksh"

#Declara una variable para mantener la fecha actual
date=$(date '+%Y-%m-%d_%H:%M:%S')

#Define un Archivo Log donde se generarán los logs
LOGFILE="/produccion/src/main/python/logs/copiar_archivos_locales_a_hdfs_${date}.log"

######################################################################
### COMENTARIOS: A partir de este punto, toda la salida estándar y el 
### error estándar se registrarán en el archivo log.
######################################################################
{  # <--- Inicio del archivo log.
echo "${JOBNAME} Se ha iniciado...: $(date)"
LOCAL_STAGING_PATH="/produccion/src/main/python/staging"
LOCAL_CITY_DIR=${LOCAL_STAGING_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_STAGING_PATH}/fact

HDFS_STAGING_PATH=/proyectos/PrescPipeline/staging
HDFS_CITY_DIR=${HDFS_STAGING_PATH}/dimension_city
HDFS_FACT_DIR=${HDFS_STAGING_PATH}/fact

### Copiar el archivo 'dimension_city' y 'fact' en HDFS
hdfs dfs -put -f ${LOCAL_CITY_DIR}/* ${HDFS_CITY_DIR}/
hdfs dfs -put -f ${LOCAL_FACT_DIR}/* ${HDFS_FACT_DIR}/
echo "${JOBNAME} ha finalizado...: $(date)"
} > ${LOGFILE} 2>&1  # <--- Fin del programa y fin del log.
