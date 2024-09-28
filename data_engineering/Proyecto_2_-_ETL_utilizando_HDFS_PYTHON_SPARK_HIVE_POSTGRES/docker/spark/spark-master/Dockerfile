FROM spark-base:latest

ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_UI_PORT=8082
ENV SPARK_MASTER_LOG_DIR=/var/logs/spark

# Instalamos unzip y la librería "psycopg2-binary" que nos permite trabajar con PostgreSQL
RUN apt-get update && apt-get install -y unzip \
    && pip install psycopg2-binary

### Explicación:
#- `apt-get update`: Actualiza la lista de paquetes disponibles.
#- `apt-get install -y unzip`: Instala `unzip` sin requerir confirmación.
#- La doble barra (`&&`) asegura que si la actualización falla, la instalación de `unzip` no se ejecutará.

COPY start-master.sh .

RUN chmod +x start-master.sh

EXPOSE 8082 
EXPOSE 7077 
EXPOSE 6066

CMD [ "./start-master.sh" ]
