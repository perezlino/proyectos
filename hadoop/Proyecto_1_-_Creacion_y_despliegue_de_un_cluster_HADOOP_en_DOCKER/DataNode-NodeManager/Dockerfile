# Usamos como base el contenedor creado anteriormente
FROM hadoop-base-image:latest

MAINTAINER Alfonso Perez Lino

# Cambiamos al usuario root
USER root

# Definimos las variables de entorno
ENV HADOOP_VERSION 3.3.6
ENV LOG_TAG "[DNNM Hadoop_${HADOOP_VERSION}]:"
ENV BASE_DIR /opt/bd
ENV HADOOP_HOME ${BASE_DIR}/hadoop/
ENV HADOOP_CONF_DIR ${HADOOP_HOME}/etc/hadoop/
ENV DATA_DIR /var/data/hadoop/hdfs

# Creamos las carpetas para los datos de HDFS del DataNode y hacemos el usuario hdadmin propietario de ellas
# En un entorno de producción se deberían indicar varias carpetas separadas
RUN echo "$LOG_TAG Creando carpetas para los datos de HDFS del DataNode" && \
    mkdir -p ${DATA_DIR}/dn && chown -R hdadmin:hadoop ${DATA_DIR}

# Creamos carpeta para los ficheros de log
RUN echo "$LOG_TAG Creando carpeta para los ficheros de log" && \
    mkdir ${HADOOP_HOME}/logs

# Copiamos los ficheros de configuración y el script de inicio
RUN echo "$LOG_TAG Copiando los ficheros de configuración y el script de inicio"
COPY Config-files/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY Config-files/hdfs-site-datanode.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY Config-files/yarn-site-nodemanager.xml ${HADOOP_CONF_DIR}/yarn-site.xml
COPY Config-files/mapred-site-nodemanager.xml ${HADOOP_CONF_DIR}/mapred-site.xml

# Script de inicio
COPY Config-files/start-daemons-dnnm.sh ${BASE_DIR}/start-daemons.sh

# Establecemos los permisos
RUN chmod +x ${BASE_DIR}/start-daemons.sh && \
    chown -R hdadmin:hadoop ${BASE_DIR}

# Establecemos los puertos para los servicios web del Datanode
EXPOSE 9864 9865 9866 9867

# Establecemos los puertos para los servicios web del NodeManager
EXPOSE 8040 8042 8044 8048

# # Establecemos los puertos para los servicios web del MapReduce
EXPOSE 50000-50050 50100-50200

# Cambiamos al usuario hdadmin
USER hdadmin

# Definimos las variables de entorno
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV HADOOP_HOME ${BASE_DIR}/hadoop/
ENV HADOOP_CONF_DIR ${HADOOP_HOME}/etc/hadoop/
ENV PATH ${PATH}:${HADOOP_HOME}/bin/:${HADOOP_HOME}/sbin/

# Establecemos el directorio de trabajo
WORKDIR ${HADOOP_HOME}

# Ejecutamos lo servicios
RUN echo "$LOG_TAG Iniciando demonios"
CMD ["/opt/bd/start-daemons.sh"]