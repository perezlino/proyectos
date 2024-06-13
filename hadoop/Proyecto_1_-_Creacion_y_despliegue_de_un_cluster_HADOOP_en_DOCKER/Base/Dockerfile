# Desplegamos una máquina con el SO Linux Ubuntu
FROM ubuntu:latest
LABEL Alfonso Perez Lino

# Cambiamos al usuario root
USER root

# Establecemos las variables
ENV HADOOP_VERSION 3.3.6
ENV BASE_DIR /opt/bd
ENV LOG_TAG "[BASE Hadoop_${HADOOP_VERSION}]:"
ENV REPOSITORY https://dlcdn.apache.org/hadoop/common

# Instalamos el software necesario para Hadoop
RUN echo "$LOG_TAG actualizando e instalando paquetes básicos" && \
    apt-get update && \
    apt-get install -y --no-install-recommends openjdk-8-jre python3 curl locales iputils-ping && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists

# Configuramos el idioma
RUN echo "$LOG_TAG configurando idioma" && \
    locale-gen es_ES.UTF-8 && update-locale LANG=es_ES.UTF-8

# Creamos una carpeta para la instalación en /opt
RUN mkdir -p ${BASE_DIR}
    
# Cambiamos a la carpeta creada
WORKDIR ${BASE_DIR}

# Descargamos la última versión de Hadoop, la 3.3.6 en /opt/bd
RUN echo "$LOG_TAG Descargando Hadoop" && \
    curl -fLO "${REPOSITORY}/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"  && \
    tar xzvf hadoop-${HADOOP_VERSION}.tar.gz && \
    ln -s hadoop-${HADOOP_VERSION} hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Creamos un grupo Hadoop y un usuario hdadmin para ejecutar los diferentes daemon de Hadoop (HDFS y YARN)
# Cambiamos el propietario del directorio de Hadoop 
RUN groupadd -r hadoop && \
    useradd hdadmin -r -g hadoop -d ${BASE_DIR} -s /bin/bash 

# Creamos una carpeta para los ficheros de log
RUN mkdir ${HADOOP_HOME}/logs

# Establecemos los permisos
RUN chown -R hdadmin:hadoop /opt/bd