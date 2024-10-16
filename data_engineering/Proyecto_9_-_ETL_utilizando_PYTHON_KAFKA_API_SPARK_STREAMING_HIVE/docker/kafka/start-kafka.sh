#!/bin/bash

# Definir la ruta de Kafka
KAFKA_HOME=/opt/kafka

# Comprobar si los archivos de configuración existen
if [[ ! -f "$KAFKA_HOME/config/zookeeper.properties" ]]; then
    echo "Error: El archivo de configuración zookeeper.properties no se encuentra."
    exit 1
fi

if [[ ! -f "$KAFKA_HOME/config/server.properties" ]]; then
    echo "Error: El archivo de configuración server.properties no se encuentra."
    exit 1
fi

# Iniciar Zookeeper
"$KAFKA_HOME/bin/zookeeper-server-start.sh" "$KAFKA_HOME/config/zookeeper.properties" &

# Esperar un momento para asegurarse de que Zookeeper esté funcionando
sleep 5

# Iniciar Kafka
exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/server.properties"

