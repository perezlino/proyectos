# Primero indicamos que nos basamos en la imagen de Ubuntu 16.04 como imagen base.
FROM hadoop-cluster-base:latest
LABEL Alfonso Perez Lino

# Cambiamos al usuario root
USER root

# Establecemos las variables
# ENV HADOOP_VERSION 3.3.6
# ENV BASE_DIR /opt/bd
ENV LOG_TAG "[NameNode & Resourcemanager Hadoop_${HADOOP_VERSION}]:" 
# ENV HADOOP_HOME ${BASE_DIR}/hadoop
# ENV HADOOP_CONF_DIR ${HADOOP_HOME}/etc/hadoop
# ENV PATH=$PATH:{HADOOP_HOME}/bin:{HADOOP_HOME}/sbin     

# Cambiamos a la carpeta /opt/bd
WORKDIR ${BASE_DIR}

#Creamos los directorios para los namenodes, los datanodes y los logs de Hadoop
RUN echo "$LOG_TAG Crear directorio namenode" && \
    mkdir -p ${BASE_DIR}/hdfs/namenode 

#Copiamos los archivos de configuración que tenemos en el directorio "Config-files" de nuestro PC al 
#directorio /opt/bd/hadoop/etc/hadoop del contenedor.
RUN echo "$LOG_TAG Copiando archivos de configuración y script de inicio"
COPY Config-files/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY Config-files/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY Config-files/yarn-site.xml ${HADOOP_CONF_DIR}/yarn-site.xml
COPY Config-files/mapred-site.xml ${HADOOP_CONF_DIR}/mapred-site.xml

# Script de inicio de daemons y SSH
COPY Config-files/start-daemons.sh ${BASE_DIR}/start-daemons.sh
COPY Config-files/start-services.sh ${BASE_DIR}/start-services.sh

# Establecemos los permisos
RUN echo "$LOG_TAG Cambiar permisos de ejecución" && \
    chmod +x ${BASE_DIR}/start-daemons.sh && \
    chmod +x ${BASE_DIR}/start-services.sh

# Formateamos el sistema de archivos de hadoop
# Esto solo debemos hacerlo en el namenode y no en los datanode
RUN echo "$LOG_TAG Formatear el sistema de archivos de hadoop"
RUN ${HADOOP_HOME}/bin/hdfs namenode -format -nonInteractive 2> /dev/null

# Puertos NameNode
EXPOSE 9870 9871

# Puertos ResourceManager
EXPOSE 8088 8090     

WORKDIR ${HADOOP_HOME}

RUN echo "$LOG_TAG Iniciando servicios"
CMD ["/opt/bd/start-services.sh"]