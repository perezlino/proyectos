function wait_for_it() {
    local serviceport=$1
    local service=${serviceport%%:*}
    local port=${serviceport#*:}
    local retry_seconds=5
    local max_try=10
    let i=1

    # Iniciar Zookeeper en segundo plano
    /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties &

    # Esperar a que Zookeeper esté disponible usando la dirección loopback
    echo "Esperando a que Zookeeper esté disponible..."
    wait_for_it "127.0.0.1:2181"  # Cambié localhost a 127.0.0.1

    # Iniciar Kafka
    echo "Iniciando Kafka..."
    exec /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties &

    # Bucle para esperar el servicio especificado
    until nc -z $service $port; do
        echo "[$i/$max_try] check for ${service}:${port}..."
        echo "[$i/$max_try] ${service}:${port} is not available yet"
        if (( $i == max_try )); then
            echo "[$i/$max_try] ${service}:${port} is still not available; giving up after ${max_try} tries. :/"
            exit 1
        fi
        
        echo "[$i/$max_try] try in ${retry_seconds}s once again ..."
        let "i++"
        sleep $retry_seconds
    done
    echo "[$i/$max_try] $service:${port} is available."
}
