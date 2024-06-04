#!/bin/bash

service ssh start

# Ejecutar el script para iniciar los daemons de mi aplicación
/opt/bd/start-daemons.sh

# Ejecutar el script para iniciar el servicio SSH
# /etc/start-ssh.sh

bash