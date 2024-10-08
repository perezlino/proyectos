FROM spark-base:latest
LABEL maintainer="Alfonso Perez Lino"

# Instalar dependencias necesarias para Kafka y Jupyter
RUN apt-get update -yqq && \
	apt-get install -yqq git wget curl vim python3-pip telnet && \
    pip3 install jupyter && \
    wget -c -O kafka-src.tgz https://archive.apache.org/dist/kafka/2.5.0/kafka_2.12-2.5.0.tgz && \
    tar xzf kafka-src.tgz -C /opt && \
    rm kafka-src.tgz && \
    mv /opt/kafka_2.12-2.5.0 /opt/kafka
	
# Crear el directorio para el kernel de PySpark
RUN mkdir -p /usr/local/share/jupyter/kernels/pyspark2

# Copiar el archivo kernel.json al directorio correspondiente
COPY kernel.json /usr/local/share/jupyter/kernels/pyspark2/

# Instalar el kernel
RUN jupyter kernelspec install /usr/local/share/jupyter/kernels/pyspark2 --user

# Copiar scripts de inicio y detención a /opt/kafka/bin
COPY ./start-kafka.sh ./
COPY ./stop-kafka.sh ./

# Dar permisos de ejecución a los scripts
RUN chmod +x ./start-kafka.sh && \
    chmod +x ./stop-kafka.sh

# Configurar Jupyter para que no requiera token
RUN mkdir /root/.jupyter && \
    echo "c.NotebookApp.token = ''" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.ip = '0.0.0.0'" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.port = 8888" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.allow_origin = '*'" >> /root/.jupyter/jupyter_notebook_config.py

# Configurar variables de entorno
ENV KAFKA_HOME=/opt/kafka
ENV PATH="$PATH:$KAFKA_HOME/bin"

# Exponer puertos de Kafka y Jupyter
EXPOSE 9092
EXPOSE 2181
EXPOSE 8888

# Comando de inicio
CMD ["sh", "-c", "./start-kafka.sh & jupyter notebook --ip 0.0.0.0 --no-browser --allow-root"]

