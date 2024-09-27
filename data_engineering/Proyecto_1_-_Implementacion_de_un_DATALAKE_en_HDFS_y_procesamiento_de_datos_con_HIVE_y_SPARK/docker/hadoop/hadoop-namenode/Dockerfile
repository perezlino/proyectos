FROM hadoop-base:latest
LABEL maintainer="Alfonso Perez Lino"

ENV HDFS_CONF_DFS_NAMENODE_DATA_DIR=file:///hadoop/dfs/name

RUN mkdir -p /hadoop/dfs/name

# Directorio que albergará el archivo "deploy_directorios.sh" 
RUN mkdir -p /hadoop-data/deploy 

# Directorio que alberagrá los schemas AVRO
RUN mkdir -p /hadoop-data/schemas

# Directorio que albergará los archivos con los que vamos a trabajar
RUN mkdir -p /hadoop-data/archivos

COPY ./start-namenode.sh ./start-namenode.sh

RUN chmod +x ./start-namenode.sh

EXPOSE 9870

CMD [ "./start-namenode.sh" ]