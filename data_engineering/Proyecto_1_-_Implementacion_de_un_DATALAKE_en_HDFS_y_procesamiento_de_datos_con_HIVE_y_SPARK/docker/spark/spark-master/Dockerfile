FROM spark-base:latest

ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_UI_PORT=8082
ENV SPARK_MASTER_LOG_DIR=/var/logs/spark

# Directorio que albergará el script de spark para la creación de la tabla "TRANSACCION_ENRIQUECIDA"
RUN mkdir -p cd /usr/local/spark/deploy

COPY start-master.sh .

RUN chmod +x start-master.sh

EXPOSE 8082 
EXPOSE 7077 
EXPOSE 6066

CMD [ "./start-master.sh" ]