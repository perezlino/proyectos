# Primero indicamos que nos basamos en la imagen de Ubuntu 16.04 como imagen base.
FROM ubuntu:latest
LABEL Alfonso Perez Lino

# Cambiamos al usuario root
USER root

# Establecemos las variables
ENV HADOOP_VERSION 3.3.6
ENV BASE_DIR /root
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

# Cambiamos a la carpeta /root
WORKDIR ${BASE_DIR}

#Ahora nos bajamos el software de Hadoop y lo dejamos en /usr/local/hadoop
RUN echo "$LOG_TAG Descargando Hadoop" && \
    wget "${REPOSITORY}/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" && \
    tar -xzvf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} /usr/local/hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

#Establecemos las variables de entorno JAVA_HOME apuntando a la ruta del JDK, 
#HADOOP_HOME apuntando a la ruta en la que hemos copiado Hadoop e incluimos en 
#la variable PATH las rutas con los binarios y scripts para gestionar Hadoop.
RUN echo "$LOG_TAG Establecer variables de entorno"
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$PATH:{HADOOP_HOME}/bin:{HADOOP_HOME}/sbin 

#Creamos las claves ssh necesarias para que se comunique el nodo master con los nodos slaves. Al tener todos las 
#mismas claves e incluirlas como claves autorizadas, el acceso ssh entre los distintos nodos está garantizado.
RUN echo "$LOG_TAG Crear claves SSH" && \
    ssh-keygen -t rsa -f ~/.ssh/id_rsa -P '' && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

#Creamos los directorios para los namenodes, los datanodes y los logs de Hadoop
RUN echo "$LOG_TAG Crear directorios namenode, datanode y logs" && \
    mkdir -p ${BASE_DIR}/hdfs/namenode && \ 
    mkdir -p ${BASE_DIR}/hdfs/datanode && \
    mkdir $HADOOP_HOME/logs

#Copiamos los archivos de configuración y scripts que tenemos en el directorio "Config-files" de nuestro PC al 
#directorio /tmp del contenedor. De ahí los vamos moviendo cada uno a su ubicación final. Más adelante explico 
#cada archivo y su contenido.
COPY Config-files/* /tmp/

RUN echo "$LOG_TAG Mover archivos" && \
    mv /tmp/start-hadoop.sh ${BASE_DIR}/start-hadoop.sh && \
    mv /tmp/hadoop-env.sh /usr/local/hadoop/hadoop-env.sh && \
    mv /tmp/hdfs-site.xml /usr/local/hadoop/hdfs-site.xml && \ 
    mv /tmp/core-site.xml /usr/local/hadoop/core-site.xml && \
    mv /tmp/mapred-site.xml /usr/local/hadoop/mapred-site.xml && \
    mv /tmp/yarn-site.xml /usr/local/hadoop/yarn-site.xml && \
    mv /tmp/slaves /usr/local/hadoop/slaves && \
    mv /tmp/ssh_config ${BASE_DIR}/.ssh/config 

RUN echo "$LOG_TAG Cambiar permisos de ejecución" && \
    chmod +x ${BASE_DIR}/start-hadoop.sh && \
    chmod +x ${HADOOP_HOME}/sbin/start-dfs.sh && \
    chmod +x ${HADOOP_HOME}/sbin/start-yarn.sh 

#Formateamos el sistema de archivos de hadoop
RUN echo "$LOG_TAG Formatear el sistema de archivos de hadoop" && \
    ${HADOOP_HOME}/bin/hdfs namenode -format -nonInteractive 2> /dev/null

#Como proceso de los contenedores configuramos el demonio de ssh. Asi el nodo master podrá acceder a los nodos slave 
#para arrancar los procesos de Hadoop necesarios para que formen parte del cluster.
CMD [ "sh", "-c", "service ssh start; bash"]

#Por último dejamos abiertos unos cuantos puertos. En principio no son necesarios todos, pero los dejo así por si en 
#un futuro los necesito.
# Hdfs ports
EXPOSE 9000 50010 50020 50070 50075 50090
EXPOSE 9871 9870 9820 9869 9868 9867 9866 9865 9864
# Mapred ports
EXPOSE 19888
#Yarn ports
EXPOSE 8030 8031 8032 8033 8040 8042 8088 8188
#Other ports
EXPOSE 49707 2122






