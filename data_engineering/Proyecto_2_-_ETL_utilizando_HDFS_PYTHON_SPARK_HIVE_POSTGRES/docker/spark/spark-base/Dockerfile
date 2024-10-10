FROM hive-base:latest
LABEL maintainer="Alfonso Perez Lino"

# Defining useful environment variables
ENV SPARK_VERSION=2.4.5
ENV HADOOP_VERSION=2.7
ENV SCALA_VERSION=2.12.4
ENV SCALA_HOME=/usr/share/scala
ENV SPARK_HOME=/usr/local/spark
ENV SBT_HOME=/usr/local/sbt
ENV SBT_VERSION=1.2.8
ENV PYTHONHASHSEED=1
ENV SPARK_EXECUTOR_MEMORY=650m
ENV SPARK_DRIVER_MEMORY=650m
ENV SPARK_WORKER_MEMORY=650m
ENV SPARK_DAEMON_MEMORY=650m
ENV PATH $SPARK_HOME/bin/:$PATH
ENV PATH $SBT_HOME/bin/:$PATH

# Upgrade and install some tools and dependencies
RUN apt-get update -yqq && \
    apt-get upgrade -yqq && \
    apt-get install -yqq \
        netcat \
        apt-utils \
        curl \
        vim \
        ssh \
        net-tools \
        ca-certificates \
        jq \
        wget \
        software-properties-common \
        python3.7 python3.7-dev python3.7-distutils \
        python3-numpy python3-matplotlib python3-scipy python3-pandas python3-simpy && \
    rm -rf /var/lib/apt/lists/*

# Installing Scala
WORKDIR /tmp
RUN wget --no-verbose "https://downloads.typesafe.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz" && \
    tar zxf scala-${SCALA_VERSION}.tgz && \
    mkdir -p ${SCALA_HOME} && \
    rm "scala-${SCALA_VERSION}/bin/"*.bat && \
    mv "scala-${SCALA_VERSION}/bin" "scala-${SCALA_VERSION}/lib" "${SCALA_HOME}" && \
    ln -s "${SCALA_HOME}/bin/"* "/usr/bin/" && \
    rm -rf scala-${SCALA_VERSION}*

# Instalación de SBT
RUN mkdir -p ${SBT_HOME} && \
    wget -qO - "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
    | tar xz -C /usr/local/sbt --strip-components=1

# Verificar la versión de SBT instalada
RUN echo -ne "- with sbt $SBT_VERSION\n" >> /root/.built && \
    sbt sbtVersion

# Establecer python3.7 como la versión predeterminada de Python
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1 && \
    update-alternatives --set python /usr/bin/python3.7

# Instalación de pip específico para Python 3.7
RUN curl https://bootstrap.pypa.io/pip/3.7/get-pip.py -o get-pip.py && \
    python3.7 get-pip.py && \
    rm get-pip.py

# Actualización de pip a una versión específica (opcional)
RUN pip install --upgrade "pip==20.2.4"

# Installing Spark
WORKDIR ${SPARK_HOME}
RUN wget --no-verbose https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar zxf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/* . && \
    rm -rf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    rm -rf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}

# Use Spark with Hive
RUN cp ${HIVE_HOME}/conf/hive-site.xml $SPARK_HOME/conf

RUN apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /tmp/* /var/tmp/*

WORKDIR /

COPY ./entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT [ "./entrypoint.sh" ]
