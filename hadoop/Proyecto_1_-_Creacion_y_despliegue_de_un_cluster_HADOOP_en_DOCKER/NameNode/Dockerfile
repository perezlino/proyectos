# Trabajamos sobre la imagen creada anteriormente
FROM hadoop-base-image:latest
LABEL Alfonso Perez Lino

# Cambiamos al usuario root
USER root

# Definimos las variables
ENV HADOOP_VERSION 3.3.6
ENV LOG_TAG "[NameNode Hadoop_${HADOOP_VERSION}]:"
ENV BASE_DIR /opt/bd
ENV HADOOP_HOME ${BASE_DIR}/hadoop
ENV HADOOP_CONF_DIR ${HADOOP_HOME}/etc/hadoop
ENV DATA_DIR /var/data/hadoop/hdfs

# Creamos las carpetas para los datos de HDFS del NameNode y hacemos el usuario hdadmin propietario de ellas 
# En un entorno de producción se deberían crear varias carpetas en particiones separadas
RUN echo "$LOG_TAG Creando carpetas para los datos de HDFS del NameNode" && \
    mkdir -p ${DATA_DIR}/nn && chown -R hdadmin:hadoop ${DATA_DIR}

# Creamos la carpeta para los ficheros de log
RUN echo "$LOG_TAG Creando la carpeta para los ficheros de log" && \
    mkdir ${HADOOP_HOME}/logs

# Copiamos los ficheros de configuración y script de inicio
RUN echo "$LOG_TAG Copiando ficheros de configuración y script de inicio"
COPY Config-files/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY Config-files/hdfs-site-namenode.xml ${HADOOP_CONF_DIR}/hdfs-site.xml

# Script de inicio
COPY Config-files/start-daemons-namenode.sh ${BASE_DIR}/start-daemons.sh

# Establecemos los permisos
RUN chmod +x ${BASE_DIR}/start-daemons.sh && \
    chown -R hdadmin:hadoop ${BASE_DIR}

# Preparamos los puertos que usaremos para los servicios web
EXPOSE 8020 9820 9870 9871

# Cambiamos al usuario hdadmin
USER hdadmin

# Definimos las variables
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
#ENV HADOOP_HOME ${BASE_DIR}/hadoop/
#ENV HADOOP_CONF_DIR ${HADOOP_HOME}/etc/hadoop/
ENV PATH ${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin

WORKDIR ${HADOOP_HOME}

RUN echo "$LOG_TAG Iniciando servicios"
CMD ["/opt/bd/start-daemons.sh"]
