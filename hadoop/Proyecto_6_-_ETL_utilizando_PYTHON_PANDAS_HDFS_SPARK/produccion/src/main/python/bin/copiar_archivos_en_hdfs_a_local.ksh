############################################################
# Developed By:                                            #
# Developed Date:                                          # 
# Script NAME:                                             #
# PURPOSE: Copy input vendor files from local to HDFS.     #
############################################################

# Declare a variable to hold the unix script name.
JOBNAME="copiar_archivos_en_hdfs_a_local.ksh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

#Define a Log File where logs would be generated
LOGFILE="/proyectos/PrescPipeline/src/main/python/logs/${JOBNAME}_${date}.log"

###########################################################################
### COMMENTS: From this point on, all standard output and standard error will
###           be logged in the log file.
###########################################################################
{  # <--- Start of the log file.
echo "${JOBNAME} Se ha iniciado...: $(date)"
LOCAL_OUTPUT_PATH="/proyectos/PrescPipeline/src/main/python/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/presc

HDFS_OUTPUT_PATH=/proyectos/PrescPipeline/output
HDFS_CITY_DIR=${HDFS_OUTPUT_PATH}/dimension_city
HDFS_FACT_DIR=${HDFS_OUTPUT_PATH}/presc

### Borrar los archivos en las rutas locales si existen
rm -f ${LOCAL_CITY_DIR}/*
rm -f ${LOCAL_FACT_DIR}/*

### Copy the City  and Fact file from HDFS to Local
hdfs dfs -get -f ${HDFS_CITY_DIR}/* ${LOCAL_CITY_DIR}/
hdfs dfs -get -f ${HDFS_FACT_DIR}/* ${LOCAL_FACT_DIR}/
echo "${JOBNAME} a finalizado...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.
