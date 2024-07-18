############################################################
# Developed By:                                            #
# Developed Date:                                          # 
# Script Name:                                             #
# PURPOSE: Copy input vendor files from local to HDFS.     #
############################################################
  
# Declare a variable to hold the unix script name.
JOBNAME="copiar_archivos_locales_a_azure.ksh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')
bucket_subdir_name=$(date '+%Y-%m-%d-%H-%M-%S')

#Define a Log File where logs would be generated
LOGFILE="/proyectos/PrescPipeline/src/main/python/logs/${JOBNAME}_${date}.log"

###########################################################################
### COMMENTS: From this point on, all standard output and standard error will
###           be logged in the log file.
###########################################################################
{  # <--- Start of the log file.
echo "${JOBNAME} Se ha iniciado...: $(date)"

### Define Local Directories
LOCAL_OUTPUT_PATH="/proyectos/PrescPipeline/src/main/python/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/presc

### Define SAS URLs
citySasUrl="https://prescpipeline.blob.core.windows.net/dimension-city/${bucket_subdir_name}?st=2022-04-18T23:42:37ss&spr=https&sv=2020-08-04&D"
prescSasUrl="https://prescpipeline.blob.core.windows.net/presc/${bucket_subdir_name}?st=2022-04-18T23:43:15Zttps&sv=2020-08-04&sr=c&sig=%2FY%3D"

### Push City  and Fact files to Azure.
azcopy copy "${LOCAL_CITY_DIR}/*" "$citySasUrl"
azcopy copy "${LOCAL_FACT_DIR}/*" "$prescSasUrl"

echo "${JOBNAME} ha finalizado...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.