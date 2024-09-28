#################################################################
# Autor: Alfonso Pérez Lino                                     #
# Fecha:                                                        #
# Objetivo: Despliegue de directorios input en HDFS. En el caso # 
#           de existir, se eliminan directorios input "staging" #
#           y se vuelven a crear. De igual forma se crea el     #
#           directorio que cumplirá el rol como base de datos   #
#           en Hive                                             #
#################################################################

# Declara una variable para contener el nombre del script unix.
JOBNAME="deploy_directorios.ksh"

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

# Verificamos si la ruta raiz existe, si la carpeta existe en HDFS la borramos
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

## Otra forma de borrar el directorio raiz ##
##
#echo "Eliminando la carpeta raíz..."
#ROOT_PATH="/proyectos/PrescPipeline/staging"
##
## Verificar si el directorio existe y eliminarlo si es necesario
#if hdfs dfs -test -d $ROOT_PATH; then
#  echo "El directorio $ROOT_PATH está disponible. Se procede a borrar."
#  hdfs dfs -rm -r -f $ROOT_PATH
#  echo "El directorio de salida HDFS $ROOT_PATH ha sido eliminado."
#fi

# Crear la estructura de carpetas para almacenar archivos de datos input en HDFS
echo "Creando la estructura de carpetas para datos input..."
hdfs dfs -mkdir -p \
  $ROOT_PATH \
  $ROOT_PATH/dimension_city \
  $ROOT_PATH/fact
echo "Estructura de carpetas para datos input creada exitosamente."

# Verificamos si la base de datos Hive existe, si la carpeta no existe, la creamos
echo "Verificando la existencia de la base de datos Hive..."
HIVE_PATH="/user/hive/warehouse/prescpipeline"
hdfs dfs -test -d $HIVE_PATH
status=$?
if [ $status == 1 ]
  then
  echo "El directorio $HIVE_PATH no está disponible. Se procede a crearla".
  hdfs dfs -mkdir -p $HIVE_PATH
  echo "El directorio para la abse de datos Hive en HDFS $HIVE_PATH ha sido creada".
fi

echo "${JOBNAME} ha finalizado...: $(date)"
} > ${LOGFILE} 2>&1  # <--- Fin del programa y fin del log.

                    