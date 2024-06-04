# Primero indicamos que nos basamos en la imagen de Ubuntu 16.04 como imagen base.
FROM ubuntu:latest
LABEL Alfonso Perez Lino

# Cambiamos al usuario root
USER root

# Establecemos las variables
ENV HADOOP_VERSION 3.3.6
ENV BASE_DIR /opt/bd
ENV LOG_TAG "[BASE Hadoop_${HADOOP_VERSION}]:"
ENV REPOSITORY https://dlcdn.apache.org/hadoop/common

#Instalamos el software necesario para Hadoop
#Instalamos un servidor SSH, JDK8, wget y Python
RUN echo "$LOG_TAG actualizando e instalando paquetes básicos" && \
    apt-get update && \
    apt-get install -y --no-install-recommends openssh-server openjdk-8-jdk wget python3 curl locales && \ 
    apt-get clean && \
    rm -rf /var/lib/apt/lists    

# Configuramos el idioma
RUN echo "$LOG_TAG configurando idioma" && \
    locale-gen es_ES.UTF-8 && update-locale LANG=es_ES.UTF-8    

# Creamos una carpeta para la instalación en /opt/bd
RUN mkdir -p ${BASE_DIR}

# Cambiamos a la carpeta /opt/bd
WORKDIR ${BASE_DIR}

# Descargamos la última versión de Hadoop, la 3.3.6 en /opt/bd
# Modifico el nombre del directorio "hadoop-3.3.6" a "hadoop"
RUN echo "$LOG_TAG Descargando Hadoop" && \
    curl -fLO "${REPOSITORY}/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

#Establecemos las variables de entorno JAVA_HOME apuntando a la ruta del JDK, 
#HADOOP_HOME apuntando a la ruta en la que hemos copiado Hadoop e incluimos en 
#la variable PATH las rutas con los binarios y scripts para gestionar Hadoop.
RUN echo "$LOG_TAG Establecer variables de entorno"
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 
ENV HADOOP_HOME ${BASE_DIR}/hadoop
ENV HADOOP_CONF_DIR ${HADOOP_HOME}/etc/hadoop
ENV PATH=$PATH:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin

#Creamos los directorios para los namenodes, los datanodes y los logs de Hadoop
RUN echo "$LOG_TAG Crear directorio logs y directorio /opt/bd/.ssh" 
RUN mkdir ${HADOOP_HOME}/logs
RUN mkdir -p /opt/bd/.ssh/

#Hadoop quiere hacer conexiones SSH pero sin necesidad de introducir contraseña cuando realiza alguna comunicacion
#con el resto de nodos. Para eso debemos intercambiar las llaves publicas entre todos los nodos (namenode y datanodes)
#La opcion más eficiente es crear las llaves en una imagen base y luego esta imagen utilizarla para crear el namenode
# y crear los datanodes, de esta manera, todos los nodos tendran acceso a las llaves.
#Creamos las claves ssh necesarias para que se comunique el nodo master con los nodos slaves. Al tener todos las 
#mismas claves e incluirlas como claves autorizadas, el acceso ssh entre los distintos nodos está garantizado.
#Para verificar que todos los nodos esten conectados debemos utilizar el comando "ssh" seguido del nombre del nodo o IP,
#entonces, debemos probarlo desde todos los nodos, por ejemplo, desde el worker "worker1" escribir "ssh hadoop-master" y 
# se debe conectar al nodo hadoop-master, o escribir "ssh worker2" y se debe conectar a ese nodo.

RUN echo "$LOG_TAG Crear claves SSH" && \
    ssh-keygen -t rsa -f ~/.ssh/id_rsa -P '' && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

#Copiamos los archivos de configuración que tenemos en el directorio "Config-files" de nuestro PC al 
#directorio /etc/ssh/config del contenedor. 
#Agregar al archivo "workers" los nombres host de los datanodes y si aparece "localhost" borrarlo
RUN echo "$LOG_TAG Copiar archivos"
COPY Config-files/sshd_config /etc/ssh/sshd_config 
COPY Config-files/workers ${HADOOP_CONF_DIR}/workers