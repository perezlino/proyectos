# Partimos del contenedor creado anteriormente
FROM hadoop-base-image:latest
LABEL Alfonso Perez Lino

# Cambiamos al usuario root
USER root

# Definimos las variables de entorno
ENV HADOOP_VERSION 3.3.6
ENV LOG_TAG "[Resourcemanager Hadoop_${HADOOP_VERSION}]:"
ENV BASE_DIR /opt/bd
ENV HADOOP_HOME ${BASE_DIR}/hadoop
ENV HADOOP_CONF_DIR ${HADOOP_HOME}/etc/hadoop

# Creamos la carpetapara los ficheros de log
RUN echo "$LOG_TAG Creando carpeta para los ficheros de log" && \
    mkdir ${HADOOP_HOME}/logs

# Copiamos los ficheros de configuración y el script de inicio
RUN echo "$LOG_TAG Copiando los ficheros de configuración y el script de inicio"
COPY Config-files/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY Config-files/yarn-site-resourcemanager.xml ${HADOOP_CONF_DIR}/yarn-site.xml
COPY Config-files/mapred-site-resourcemanager.xml ${HADOOP_CONF_DIR}/mapred-site.xml

# Script de inicio 
COPY Config-files/start-daemons-resourcemanager.sh ${BASE_DIR}/start-daemons.sh

# Establecemos los permisos
RUN chmod +x ${BASE_DIR}/start-daemons.sh && \
    chown -R hdadmin:hadoop ${BASE_DIR}

# Establecemos los puertos que se usarán para los servicios web
EXPOSE 8030 8031 8032 8033 8088 8090  

# Creamos el usuario no privilegiado
RUN useradd -u 2000 luser

# Cambiamos al usuario hdadmin
USER hdadmin

# Definimos las variables de entorno
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME ${BASE_DIR}/hadoop
ENV HADOOP_CONF_DIR ${HADOOP_HOME}/etc/hadoop
ENV PATH ${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin

# Cambiamos al directorio de trabajo
WORKDIR ${HADOOP_HOME}

# Ejecutamos los servicios
RUN echo "$LOG_TAG Iniciando demonios"
CMD ["/opt/bd/start-daemons.sh"]