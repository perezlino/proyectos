############################################################
# Developed By:                                            #
# Developed Date:                                          # 
# Script NAME:                                             #
# PURPOSE:                                                 #
############################################################

# Declare a variable to hold the unix script name.
JOBNAME="deploy_directorios.ksh"

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

# Verificamos si la ruta raiz existe, si la carpeta existe la borramos
echo "Eliminando carpeta raiz..."
ROOT_PATH="/proyectos/PrescPipeline/staging"
hdfs dfs -test -d $ROOT_PATH
status=$?
if [ $status == 0 ]
  then
  echo "El directorio $ROOT_PATH está disponible. Se procede a borrar".
  hdfs dfs -rm -r -f $ROOT_PATH
  echo "El directorio de salida HDFS $ROOT_PATH ha sido eliminado".
fi

#Estructura de carpetas para almacenar archivos de datos input
echo "Creando la estructura de carpetas para datos input..."
hdfs dfs -mkdir -p \
/proyectos/PrescPipeline/staging \
/proyectos/PrescPipeline/staging/dimension_city \
/proyectos/PrescPipeline/staging/fact \

# Verificamos si la base de datos Hive existe, si la carpeta no existe la creamos
echo "Verificando la existencia de la base de datos Hive..."
HIVE_PATH="/user/hive/warehouse/prescpipeline"
hdfs dfs -test -d $HIVE_PATH
status=$?
if [ $status == 1 ]
  then
  echo "El directorio $HIVE_PATH no está disponible. Se procede a crearla".
  hdfs dfs -mkdir -p $HIVE_PATH
  echo "El directorio para la abse de datos Hive en HDFS $HIVE_PATH ha sido cresda".
fi

echo "${JOBNAME} ha finalizado...: $(date)"
} > ${LOGFILE} 2>&1  # <--- Fin del programa y fin del log.

                    