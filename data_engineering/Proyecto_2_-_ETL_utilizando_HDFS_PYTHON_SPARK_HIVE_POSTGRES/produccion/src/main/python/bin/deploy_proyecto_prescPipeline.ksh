#################################################################
# Autor: Alfonso Pérez Lino                                     #
# Fecha:                                                        #
# Objetivo: Script maestro para ejecutar todo el proyecto       #
#################################################################

PROJ_FOLDER="/produccion/src/main/python"

####################################################################################################
###
### Antes de ejecutar este script debo tener los siguientes archivos en
### mi sistema local en las siguientes rutas
### - /produccion/src/main/python/staging/dimension_city/us_cities_dimension.parquet
### - /produccion/src/main/python/staging/fact/USA_Presc_Medicare_Data_2021.csv
###
####################################################################################################

# Parte 1
# Eliminar el archivo log
printf "\nEliminando archivos log en $(date +'%d/%m/%Y_%H:%M:%S') ... \n"
rm -f ${PROJ_FOLDER}/logs/*
printf "Eliminación de archivos log finalizado en $(date +'%d/%m/%Y_%H:%M:%S') !!! \n\n"

# Crear directorios staging (directorios input) en HDFS
printf "Creando directorios staging en HDFS en $(date +'%d/%m/%Y_%H:%M:%S') ... \n"
"${PROJ_FOLDER}/bin/deploy_directorios_hdfs.ksh"
printf "Ejecución del script deploy_directorios_hdfs.ksh a finalizado en $(date +'%d/%m/%Y_%H:%M:%S') !!! \n\n"

# Copiar archivos locales a directorios input en HDFS
printf "Ejecutando el script copiar_archivos_locales_a_hdfs.ksh en $(date +'%d/%m/%Y_%H:%M:%S') ... \n"
"${PROJ_FOLDER}/bin/copiar_archivos_locales_a_hdfs.ksh"
printf "Ejecución del script copiar_archivos_locales_a_hdfs.ksh a finalizado en $(date +'%d/%m/%Y_%H:%M:%S') !!! \n\n"

# Ejecutar script para eliminar directorios output en HDFS
printf "Ejecutando el script eliminar_rutas_output_hdfs.ksh en $(date +'%d/%m/%Y_%H:%M:%S') ... \n"
"${PROJ_FOLDER}/bin/eliminar_rutas_output_hdfs.ksh"
printf "Ejecución del script eliminar_rutas_output_hdfs.ksh a finalizado en $(date +'%d/%m/%Y_%H:%M:%S') !!! \n\n"

# Llamar al job de Spark para extraer archivos Fact y City
printf "Ejecutando el script run_presc_pipeline.py en $(date +'%d/%m/%Y_%H:%M:%S') ... \n"
spark-submit --master spark://spark-master:7077 --jars ${PROJ_FOLDER}/lib/postgresql-42.2.23.jar run_presc_pipeline.py
printf "Ejecución del script run_presc_pipeline.py a finalizado en $(date +'%d/%m/%Y_%H:%M:%S') !!! \n\n"

# Parte 2
# Copiar archivos resultantes en HDFS a directorios locales
printf "Ejecutando el script copiar_archivos_en_hdfs_a_local.ksh en $(date +'%d/%m/%Y_%H:%M:%S') ... \n"
"${PROJ_FOLDER}/bin/copiar_archivos_en_hdfs_a_local.ksh"
printf "Ejecución del script copiar_archivos_en_hdfs_a_local.ksh a finalizado en $(date +'%d/%m/%Y_%H:%M:%S') !!! \n\n"
