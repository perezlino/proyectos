FROM base-hive:latest
LABEL maintainer="Alfonso Perez Lino"

COPY startup.sh .

ENV PYTHON_CMD=/usr/bin/python

RUN chmod +x startup.sh

EXPOSE 50111

CMD [ "./startup.sh" ]