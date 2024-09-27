FROM hive-base:latest

# Directorio que albergará los archivos deploy de tablas
RUN mkdir -p /opt/hive/deploy

COPY startup.sh .

RUN chmod +x startup.sh

CMD [ "./startup.sh" ]