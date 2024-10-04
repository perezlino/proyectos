FROM base-hadoop:latest
LABEL maintainer="Alfonso Perez Lino"

COPY start-resourcemanager.sh start-resourcemanager.sh

RUN chmod +x start-resourcemanager.sh

EXPOSE 8088

CMD [ "./start-resourcemanager.sh" ]