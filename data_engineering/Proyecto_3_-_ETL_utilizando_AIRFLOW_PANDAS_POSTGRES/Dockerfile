FROM apache/airflow:2.1.2

COPY requirements.txt /requirements.txt

#Esta línea actualiza 'pip' a la última versión para el usuario actual en el contenedor.
#pip install: Este es el comando de Python para instalar paquetes.
#--user: Esta opción instala el paquete solo para el usuario actual en lugar de a nivel del sistema. Esto es útil si no tienes permisos de administrador o si deseas evitar conflictos con otros entornos de Python.
#--upgrade: Esta opción actualiza el paquete a la última versión disponible.
#pip: Es el propio gestor de paquetes de Python que se está actualizando.
RUN pip install --user --upgrade pip

#Esta línea instala todos los paquetes listados en 'requirements.txt' para el usuario actual, sin usar la caché, lo que ayuda a mantener la imagen más ligera.
#pip install: Se utiliza nuevamente para instalar paquetes de Python.
#--no-cache-dir: Esta opción le indica a 'pip' que no use la caché local para almacenar los paquetes descargados. Esto ayuda a reducir el tamaño de la imagen final y evita problemas con archivos en caché.
#--user: Al igual que en el primer comando, instala los paquetes solo para el usuario actual.
#-r /requirements.txt: Esta opción le indica a 'pip' que lea los nombres de los paquetes y sus versiones desde el archivo `requirements.txt`, que debe estar ubicado en la raíz del sistema de archivos del contenedor.
RUN pip install --no-cache-dir --user -r /requirements.txt