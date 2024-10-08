#!/bin/bash

# Función para detener un servicio y mostrar un mensaje
stop_service() {
    local service_name=$1
    local stop_command=$2

    if pgrep -f "$service_name" > /dev/null; then
        echo "Deteniendo $service_name..."
        $stop_command
        echo "$service_name detenido."
    else
        echo "$service_name no está en ejecución."
    fi
}

# Detener Kafka
stop_service "kafka.Kafka" "/opt/kafka/bin/kafka-server-stop.sh"

# Detener Zookeeper
stop_service "org.apache.zookeeper.server.quorum.QuorumPeerMain" "/opt/kafka/bin/zookeeper-server-stop.sh"
