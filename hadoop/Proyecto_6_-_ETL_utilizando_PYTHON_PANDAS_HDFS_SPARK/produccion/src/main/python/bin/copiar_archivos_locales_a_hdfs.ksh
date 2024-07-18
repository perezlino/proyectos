############################################################
# Developed By: Alfonso Pérez Lino                         #
# Developed Date: 11-07-2024                               # 
# PURPOSE:                                                 #
############################################################

# Declara una variable para contener el nombre del script unix.
JOBNAME="copiar_archivos_locales_a_hdfs.ksh"

#Declara una variable para mantener la fecha actual
date=$(date '+%Y-%m-%d_%H:%M:%S')

#Define un Archivo Log donde se generarán los logs
LOGFILE="/proyectos/PrescPipeline/src/main/python/logs/copiar_archivos_locales_a_hdfs_${date}.log"

###########################################################################
### COMENTARIOS: A partir de este punto, toda la salida estándar y el error estándar se
### se registrarán en el archivo log.
###########################################################################
{  # <--- Inicio del archivo log.
echo "${JOBNAME} Se ha iniciado...: $(date)"
LOCAL_STAGING_PATH="/proyectos/PrescPipeline/src/main/python/staging"
LOCAL_CITY_DIR=${LOCAL_STAGING_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_STAGING_PATH}/fact

HDFS_STAGING_PATH=/proyectos/PrescPipeline/staging
HDFS_CITY_DIR=${HDFS_STAGING_PATH}/dimension_city
HDFS_FACT_DIR=${HDFS_STAGING_PATH}/fact

### Copy the City  and Fact file to HDFS
hdfs dfs -put -f ${LOCAL_CITY_DIR}/* ${HDFS_CITY_DIR}/
hdfs dfs -put -f ${LOCAL_FACT_DIR}/* ${HDFS_FACT_DIR}/
echo "${JOBNAME} ha finalizado...: $(date)"
} > ${LOGFILE} 2>&1  # <--- Fin del programa y fin del log.
