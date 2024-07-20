############################################################
# Developed By:                                            #
# Developed Date:                                          # 
# Script Name:                                             #
# PURPOSE: Copy input vendor files from local to HDFS.     #
############################################################

# Declare a variable to hold the unix script name.
JOBNAME="copiar_archivos_locales_a_s3.ksh"

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

LOCAL_OUTPUT_PATH="/proyectos/PrescPipeline/src/main/python/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/presc


### Push City and Fact files to s3.
### El patrón *.* significa "cualquier nombre de archivo que tenga al menos un carácter 
### antes de un punto y al menos un carácter después del punto". En términos prácticos, 
### esto seleccionará todos los archivos que tienen una extensión (por ejemplo, archivo.txt, 
### documento.docx, etc.).
for file in ${LOCAL_CITY_DIR}/*.*
do
  aws s3 --profile myprofile cp ${file} "s3://prescpipeline/dimension_city/$bucket_subdir_name/"
  echo "City File $file es empujado a S3."
done

for file in ${LOCAL_FACT_DIR}/*.*
do
  aws s3 --profile myprofile cp ${file} "s3://prescpipeline/presc/$bucket_subdir_name/"
  echo "Presc file $file es empujado a S3."
done

echo "${JOBNAME} ha finalizado...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.