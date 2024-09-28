#################################################################
# Autor: Alfonso Pérez Lino                                     #
# Fecha:                                                        #
# Objetivo: Eliminar las rutas 'output' de HDFS para que la     #
#           extracción realizada por Spark sea fluida.          #
#################################################################

# Declara una variable para contener el nombre del script unix.
JOBNAME="eliminar_rutas_output_hdfs.ksh"

#Declara una variable para mantener la fecha actual
date=$(date '+%Y-%m-%d_%H-%M-%S')

#Define un Archivo Log donde se generarán los logs
LOGFILE="/produccion/src/main/python/logs/${JOBNAME}_${date}.log"

######################################################################
### COMENTARIOS: A partir de este punto, toda la salida estándar y el 
### error estándar se registrarán en el archivo log.
######################################################################
{  # <--- Inicio del archivo log.
echo "${JOBNAME} Se ha iniciado...: $(date)"

CITY_PATH="/proyectos/PrescPipeline/output/dimension_city"
hdfs dfs -test -d "$CITY_PATH"
status=$?
if [ $status -eq 0 ]; then
  echo "El directorio de salida HDFS $CITY_PATH está disponible. Se procede a borrar."
  hdfs dfs -rm -r -f "$CITY_PATH"
  echo "El directorio de salida HDFS $CITY_PATH se elimina antes de la extracción !!!"
fi

FACT_PATH="/proyectos/PrescPipeline/output/presc"
hdfs dfs -test -d "$FACT_PATH"
status=$?
if [ $status -eq 0 ]; then
  echo "El directorio de salida HDFS $FACT_PATH está disponible. Se procede a borrar."
  hdfs dfs -rm -r -f "$FACT_PATH"
  echo "El directorio de salida HDFS $FACT_PATH se elimina antes de la extracción !!!"
fi

HIVE_CITY_PATH="/user/hive/warehouse/prescpipeline/df_city_final"
hdfs dfs -test -d "$HIVE_CITY_PATH"
status=$?
if [ $status -eq 0 ]; then
  echo "El directorio de salida HDFS $HIVE_CITY_PATH está disponible. Se procede a borrar."
  hdfs dfs -rm -r -f "$HIVE_CITY_PATH"
  echo "El directorio de salida HDFS $HIVE_CITY_PATH se ha eliminado para Hive !!!"
fi

HIVE_FACT_PATH="/user/hive/warehouse/prescpipeline/df_presc_final"
hdfs dfs -test -d "$HIVE_FACT_PATH"
status=$?
if [ $status -eq 0 ]; then
  echo "El directorio de salida HDFS $HIVE_FACT_PATH está disponible. Se procede a borrar."
  hdfs dfs -rm -r -f "$HIVE_FACT_PATH"
  echo "El directorio de salida HDFS $HIVE_FACT_PATH se ha eliminado para Hive !!!"
fi

echo "${JOBNAME} ha finalizado...: $(date)"

} > "${LOGFILE}" 2>&1  # <--- End of program and end of log.

