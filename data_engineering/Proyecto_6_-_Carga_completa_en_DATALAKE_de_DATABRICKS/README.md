# Proyecto de implementación de una Arquitectura Data Lake en Databricks

[![p471.png](https://i.postimg.cc/02b90xJ9/p471.png)](https://postimg.cc/Tpv8Tz9s)

Este proyecto se centra en la implementación de un Data Lake utilizando Databricks, con el objetivo de almacenar, procesar y analizar datos de Fórmula 1. En primera instancia creamos un diseño donde recopilamos todos los datos del contenedor **raw**, los procesamos y actualizamos tanto el contenedor **processed** como el contenedor **presentation** en cada iteración. Esta arquitectura, que llamamos **Full Load**, es sencilla y resulta eficiente para gestionar pequeñas cantidades de datos. El proceso de **Ingesta** permite añadir tanto nuevos datos como datos ya existentes al Data Lake, evitando duplicados a través de un mecanismo de sobreescritura (override). Este enfoque elimina los datos existentes y reemplaza la información anterior con los nuevos registros. De manera similar, se aplica un enfoque equivalente durante el proceso de **Transformación**.

En segunda instancia, hemos desarrollado un diseño de carga incremental que considera escenarios de re-ejecución. En lugar de sobrescribir todo el Data Lake con nuevos datos, este enfoque permite reemplazar solo aquellos registros que realmente necesitan ser actualizados. Esto es especialmente crucial en situaciones del mundo real, donde ciertos datos pueden faltar en una carga inicial pero pueden llegar posteriormente. Por lo tanto, nuestro sistema debe ser lo suficientemente inteligente para extraer solo los datos necesarios de los Data Lakes y actualizarlos en caso de re-ejecuciones.

Implementar cargas incrementales puede ser complejo, pero con este diseño, buscamos optimizar el flujo de datos, garantizando que siempre trabajemos con la información más relevante y actualizada.

## **Paso 1: Creación de Contenedores en Azure Data Lake Storage (ADLS)**

Este paso consiste en la configuración inicial de la cuenta de almacenamiento `formula1dl` en Azure Data Lake Storage. Se crean tres contenedores: `raw`, `processed` y `presentation`, que servirán para organizar y gestionar los datos de manera eficiente a lo largo del proyecto.

### Pasos Incluidos:
- Creación de una cuenta de almacenamiento en Azure denominada `formula1dl`.
- Creación del contenedor `raw` para almacenar los datos en crudo.
- Creación del contenedor `processed` para los datos procesados.
- Creación del contenedor `presentation` para los datos listos para su presentación y análisis.

## **Paso 2: Integración con Azure Data Lake Storage (ADLS)**

Este paso consiste en la integración de Azure con Databricks. Esto incluye la configuración de un Azure Active Directory (AAD) Service Principal, que facilita la autenticación y el acceso seguro a ADLS. También se crea un Azure Key Vault para gestionar de forma segura las credenciales necesarias para acceder a los datos.

### Pasos Incluidos:
- Creación de un Service Principal en Azure para la autenticación.
- Configuración de un Key Vault para almacenar y gestionar secretos.
- Asignación de permisos adecuados para el Service Principal en Azure.
- Creación de un Secret Scope en Databricks para acceder a los secretos de forma segura.

## **Paso 3: Ingestión de Datos en el Data Lake**

Una vez configurada la integración, se procede a la ingesta de datos en el Data Lake. Se crean las bases de datos necesarias para organizar los datos crudos y procesados. A través de la carga manual de archivos desde ADLS, los datos se transforman y almacenan en formato Parquet, optimizando el espacio y el rendimiento.

### Pasos Incluidos:
- Creación de las bases de datos `f1_raw` y `f1_processed` en el Data Lake.
- Ingesta de archivos en diferentes formatos (CSV y JSON) desde ADLS.
- Aplicación de transformaciones a los datos para asegurar su calidad y consistencia.

## **Paso 4: Transformación de Datos**

En esta etapa, se realizan diversas transformaciones sobre los datos almacenados en el Data Lake. Se renombran columnas, se aplican funciones de unión y se utilizan agregaciones para generar nuevos conjuntos de datos que faciliten el análisis. También se implementan funciones de ventana para obtener métricas adicionales.

### Pasos Incluidos:
- Creación de la base de datos `f1_presentation` para almacenar los resultados finales del análisis.
- Transformación de datos utilizando técnicas avanzadas de SQL.
- Generación de archivos Parquet con los resultados procesados, listos para su análisis.

## **Paso 5: Análisis de Datos**

Finalmente, se establece la capa de **presentation**, que permite a los analistas y científicos de datos realizar consultas SQL y acceder a insights significativos de los datos almacenados en el Data Lake. Esta capa está diseñada para ser intuitiva, mejorando la eficiencia en el análisis y la generación de informes.

## Despliegue del Proyecto

Aquí tienes el paso 1 de tu proyecto con un título:

### Paso 1: Creación de Contenedores en Azure Data Lake Storage

Antes de proceder con este paso, es necesario crear una cuenta de almacenamiento configurada para Azure Data Lake Storage (ADLS) y nombrarla `formula1dl`. Una vez completada esta tarea, crearemos tres contenedores en Azure Data Lake Storage: `raw`, `processed` y `presentation`. A continuación, se detallan los pasos necesarios para llevar a cabo este proceso:

1. **Accede a Azure Portal**:
   - Inicia sesión en tu cuenta de Azure en [portal.azure.com](https://portal.azure.com).

2. **Navega a la Cuenta de Almacenamiento**:
   - En el panel de navegación, busca y selecciona "Cuentas de almacenamiento".
   - Encuentra y selecciona la cuenta de almacenamiento `formula1dl` que está configurada para Azure Data Lake Storage.

3. **Accede a la Sección de Contenedores**:
   - En la página de la cuenta de almacenamiento, busca y selecciona "Contenedores" en el menú lateral.

4. **Crear el Contenedor "raw"**:
   - Haz clic en el botón "+ Contenedor" en la parte superior de la página.
   - En el cuadro de diálogo que aparece, ingresa el nombre `raw`.
   - Configura las opciones de nivel de acceso (puedes dejarlo en "Privado" si no deseas que sea accesible públicamente).
   - Haz clic en "Crear".

5. **Repetir para "processed" y "presentation"**:
   - Repite el proceso anterior para crear los otros dos contenedores:
     - Para el contenedor "processed", haz clic en "+ Contenedor", ingresa `processed` y luego haz clic en "Crear".
     - Para el contenedor "presentation", haz clic en "+ Contenedor", ingresa `presentation` y luego haz clic en "Crear".

6. **Confirmar la Creación**:
   - Después de crear los tres contenedores, deberías verlos listados en la sección de contenedores de tu cuenta de almacenamiento.


### Paso 2: Integración con ADLS

Integrar Azure con Databricks implica varios pasos, desde la creación de un Azure Active Directory (AAD) Service Principal hasta la configuración de Databricks. Te muestro el paso a paso:

#### a) Crear un Azure Active Directory (AAD) Service Principal

1.- **Inicia sesión en Azure Portal**:
   - Ve a [Azure Portal](https://portal.azure.com/) e inicia sesión con tu cuenta de Azure.

2.- **Crea un Service Principal**:
   - Navega a **Azure Active Directory**.
   - En el menú de la izquierda, selecciona **App registrations**.
   - Haz clic en **New registration**.
        - **Name**: Elige un nombre para tu aplicación.
        - **Supported account types**: Selecciona el tipo de cuenta que necesitas (normalmente "Accounts in this organizational directory only").
        - Haz clic en **Register**.

3.- **Obtener el Client ID y Tenant ID**:
   - Una vez registrada, se abrirá la página de tu aplicación. Copia el **Application (client) ID** y el **Directory (tenant) ID**.

4.- **Crear un secreto del cliente**:
   - En el menú de la izquierda, selecciona **Certificates & secrets**.
   - Haz clic en **New client secret**.
     - Escribe una descripción (nombre) y establece la duración.
     - Haz clic en **Add**.
   - Copia el valor del secreto que aparece; lo necesitarás más adelante.

#### b) Crear un Azure Key Vault

1.- **Crea un Key Vault**:
   - En el portal de Azure, selecciona **Create a resource**.
   - Busca y selecciona **Key Vault**.
   - Completa la información requerida:
     - **Name**: Elige un nombre único para el Key Vault.
     - **Subscription**: Selecciona tu suscripción.
     - **Resource group**: Selecciona un grupo de recursos existente o crea uno nuevo.
     - **Region**: Selecciona la región donde deseas crear el Key Vault.
   - Haz clic en **Review + Create** y luego en **Create**.

2.- **Agrega secretos al Key Vault**:
   - Ve al recurso del Key Vault que acabas de crear.
   - En el menú de la izquierda, selecciona **Secrets**.
   - Haz clic en **Generate/Import**.
   - Para cada secreto (Client ID, Tenant ID, Client Secret):
     - **Name**: Ingresa un nombre descriptivo (ej. `ClientID`, `TenantID`, `ClientSecret`).
     - **Value**: Introduce el password correspondiente de cada uno.
     - Haz clic en **Create**.

3.- **Copia el Vault URI y el Resource ID**:
   - En la página principal de tu Key Vault, en el menú de la izquierda, selecciona **Properties**
   - Busca **Vault URI** y copialo.
   - Busca **Resource ID** y copialo.

#### c) Asignar permisos al Service Principal

1.- **Accede a tu suscripción de Azure**:
   - Ve a **Subscriptions** en el portal de Azure.
   - Selecciona la suscripción a la que deseas acceder.

2.- **Asigna un rol al Service Principal**:
   - En la barra de herramientas, selecciona **Access control (IAM)**.
   - Haz clic en **Add role assignment**.
   - Selecciona un rol apropiado (por ejemplo, **Contributor** o **Databricks Admin**).
   - En **Assign access to**, selecciona **Azure AD user, group, or service principal**.
   - Busca tu Service Principal, selecciónalo y haz clic en **Save**.

#### d) Crear un Secret Scope en Databricks

1.- **Accede al workspace de Databricks**:
   - Inicia sesión en tu workspace de Databricks.

2.- **Crea el Secret Scope**:
   - Haz clic en el símbolo de Databricks en la esquina superior izquierda.
   - En la barra de direcciones de tu navegador, agrega `#secrets/createScope` al final de la URL. Por ejemplo:
     ```
     https://<your-databricks-workspace>#secrets/createScope
     ```

3.- **Configura el Secret Scope**:
   - Se abrirá la página para crear un nuevo Secret Scope.
   - **Nombre del Secret Scope**: Ingresa un nombre para tu Secret Scope (por ejemplo, `my_secret_scope`).
   - **Manage Principal**: Selecciona `All users`. Solo si tienes cuenta Premium podras seleccionar `Creator`
   - Para **Azure Key Vault** debemos indicar lo siguiente:
   - **DNS Name**: Pega el **Vault URI** que copiaste de Azure Key Vault.
   - **Resource ID**: Pega el **Resource ID** que también copiaste.

4.- **Crear el Secret Scope**:
   - Haz clic en **Create** para finalizar la creación del Secret Scope.

#### e) Acceder a los secretos en Databricks

1.- **Utiliza los secretos en un notebook**:
   - En un notebook, puedes acceder a los secretos utilizando el siguiente código:

     ```python
     client_id = dbutils.secrets.get(scope="my_secret_scope", key="ClientID")
     tenant_id = dbutils.secrets.get(scope="my_secret_scope", key="TenantID")
     client_secret = dbutils.secrets.get(scope="my_secret_scope", key="ClientSecret")
     ```
   - **scope**: Corresponde al nombre de tu Databricks Secret Scope
   - **key**: Corresponde al nombre del secreto que le diste al `ClienteID`, `TenantID` y `ClientSecret` en Azure Key Vault.

### Paso 3: Ingestión para Carga completa

La carga completa está diseñada para procesar un único archivo de forma continua. Esto significa que cada vez que se actualice este archivo, realizaremos el proceso de ingestión de todos los datos nuevamente. Es importante destacar que de los ocho archivos disponibles, solo cuatro se actualizarán diariamente: **lap_times**, **pit_stops**, **qualifying** y **results**. Los otros cuatro archivos solo requieren una carga inicial. No obstante, dado su tamaño reducido, es irrelevante volver a cargarlos en cada ejecución de la carga completa.

Si tuviéramos un directorio para cada archivo, el proceso sería similar, con algunas pequeñas modificaciones. Si el archivo llegara con la fecha especificada en su nombre, podríamos añadir una columna para capturar ese valor, lo que nos permitiría identificar de manera clara a qué archivo (o fecha) pertenecen los datos en particular.

#### Crear bases de datos y tablas 

Comienza el proceso estableciendo la base de datos `f1_raw`, que servirá para almacenar los datos en bruto. Luego, crea la base de datos `f1_processed`, que será el destino para los datos ingeridos durante el proceso de transformación. Una vez creadas las bases de datos, procederás a definir las tablas que almacenarán los datos crudos. Las tablas a crear son: `circuits`, `results`, `constructors`, `pit_stops`, `lap_times`, `drivers`, `races` y `qualifying`. Con esta estructura, estarás preparado para cargar los datos crudos en la base de datos `f1_raw`, desde donde podrás proceder a transformarlos y procesarlos para almacenarlos en `f1_processed` según sea necesario.

#### Carga manual de datos 

Procederemos a realizar la carga manual de datos, cargando archivos de diversos formatos en el contenedor `raw` en Azure Data Lake Storage (ADLS). Si no cuentas con una cuenta en Azure, puedes ejecutar el proyecto en Databricks Community, lo que ofrece una alternativa viable para trabajar con los datos sin las restricciones de una cuenta paga. Sin embargo, el proyecto se verá limitado en muchos aspectos, especialmente en el despliegue del notebook que contiene el workflow. 

<center><img src="https://images2.imgbox.com/a1/9f/t0LTa2k6_o.png"></center> <!--db61-->


##### Carga de archivos hacia la ruta de ingesta (Utilizando ADLS): 

Aquí tienes un paso a paso para subir archivos a un contenedor llamado "raw" en Azure Data Lake:

1. **Accede a Azure Portal**:
   - Inicia sesión en tu cuenta de Azure en [portal.azure.com](https://portal.azure.com).

2. **Navega a tu Azure Data Lake Storage**:
   - En el panel de navegación, busca y selecciona "Cuentas de almacenamiento".
   - Selecciona la cuenta de almacenamiento `formula1dl`.

3. **Accede al contenedor**:
   - En la página de tu cuenta de almacenamiento, selecciona `Contenedores` en el menú lateral.
   - Busca y selecciona el contenedor llamado `raw`.

4. **Sube los archivos**:
   - En la parte superior de la página del contenedor, haz clic en el botón "Cargar".
   - Se abrirá un cuadro de diálogo donde puedes arrastrar y soltar archivos o hacer clic en "Navegar" para seleccionar archivos de tu sistema local.
   - Selecciona los archivos que deseas subir y haz clic en "Cargar".

5. **Confirmar la Carga**:
   - Una vez que los archivos se hayan cargado, podrás verlos listados en el contenedor `raw`.


##### Carga de archivos hacia la ruta de ingesta (En el caso de no tener una cuenta en Azure, utiliza Databricks Community): 

Si vas a utilizar Databricks Community, cuando utilizas la interfaz de usuario para cargar archivos, a menudo se limita la ubicación de destino a directorios predeterminados, como `/FileStore/tables/`. Sin embargo, si deseas cargar archivos directamente a una ubicación específica como `/mnt`, puedes hacerlo a través de otros métodos. Aquí tienes cómo hacerlo:

1. **Sube a `/FileStore`**: Si la interfaz solo te permite cargar a `/FileStore/tables/`, primero sube los archivos allí.

2. **Mover el Archivo**: Después de subir el archivo, puedes moverlo a la ruta `/mnt/formula1dl/raw` utilizando `dbutils`:

```python
# Mover archivos desde FileStore a /mnt/formula1d1/raw
# La ruta /mnt/formula1dl/raw/lap_times al momento de la copia se crea automaticamente
# La ruta /mnt/formula1dl/raw/qualifying al momento de la copia se crea automaticamente
dbutils.fs.cp("dbfs:/FileStore/tables/circuits.csv", "dbfs:/mnt/formula1dl/raw/circuits.csv")
dbutils.fs.cp("dbfs:/FileStore/tables/constructors.json", "dbfs:/mnt/formula1dl/raw/constructors.json")
dbutils.fs.cp("dbfs:/FileStore/tables/drivers.json", "dbfs:/mnt/formula1dl/raw/drivers.json")
dbutils.fs.cp("dbfs:/FileStore/tables/pit_stops.json", "dbfs:/mnt/formula1dl/raw/pit_stops.json")
dbutils.fs.cp("dbfs:/FileStore/tables/races.csv", "dbfs:/mnt/formula1dl/raw/races.csv")
dbutils.fs.cp("dbfs:/FileStore/tables/lap_times_split_1.csv", "dbfs:/mnt/formula1dl/raw/lap_times/lap_times_split_1.csv")
dbutils.fs.cp("dbfs:/FileStore/tables/lap_times_split_2.csv", "dbfs:/mnt/formula1dl/raw/lap_times/lap_times_split_2.csv")
dbutils.fs.cp("dbfs:/FileStore/tables/lap_times_split_3.csv", "dbfs:/mnt/formula1dl/raw/lap_times/lap_times_split_3.csv")
dbutils.fs.cp("dbfs:/FileStore/tables/lap_times_split_4.csv", "dbfs:/mnt/formula1dl/raw/lap_times/lap_times_split_4.csv")
dbutils.fs.cp("dbfs:/FileStore/tables/lap_times_split_5.csv", "dbfs:/mnt/formula1dl/raw/lap_times/lap_times_split_5.csv")
dbutils.fs.cp("dbfs:/FileStore/tables/qualifying_split_1.json", "dbfs:/mnt/formula1dl/raw/qualifying/qualifying_split_1.json")
dbutils.fs.cp("dbfs:/FileStore/tables/qualifying_split_2.json", "dbfs:/mnt/formula1dl/raw/qualifying/qualifying_split_2.json")
```

#### Procesar y guardar resultados 

Aplicar transformaciones a los archivos ingeridos y guardar los resultados en la base de datos `f1_processed`, que servirá como fuente para el siguiente paso del proceso.

##### Ingesta del archivo "circuits.csv"

1. Leer el archivo
2. Seleccionar solo las columnas que necesitamos
3. Cambiar el nombre de ciertas columnas
4. Añadir la fecha de ingestión al dataframe
5. Escribir datos en el contenedor **processed** del ADLS como **parquet**

<center><img src="https://i.postimg.cc/B6dF4Thd/db58.png"></center>

##### Ingesta del archivo "races.csv"

1. Leer el archivo
2. Añadir las columnas *ingestion_date* y *race_timestamp*
3. Seleccionar sólo las columnas necesarias y renombrarlas como corresponda
4. Escribir datos en el contenedor **processed** del ADLS como **parquet**

<center><img src="https://i.postimg.cc/PqNskYvb/db59.png"></center>

##### Ingesta del archivo "constructors.json"

1. Leer el archivo
2. Eliminar columnas no deseadas
3. Cambiar el nombre de las columnas y añadir "ingestion date"
4. Escribir datos en el contenedor **processed** del ADLS como **parquet**

<center><img src="https://images2.imgbox.com/44/b5/oUpevSA8_o.png"></center> <!--db62-->

##### Ingesta del archivo "drivers.json"

1. Leer el archivo
2. Renombrar columnas y añadir nuevas columnas
3. Eliminar columnas no deseadas
4. Escribir datos en el contenedor **processed** del ADLS como **parquet**

<center><img src="https://images2.imgbox.com/e4/f7/eGqClVvp_o.png"></center> <!--db63-->

##### Ingesta del archivo "results.json"

1. Leer el archivo
2. Renombrar columnas y añadir nuevas columnas
3. Eliminar las columnas no deseadas
4. Escribir datos en el contenedor **processed** del ADLS como **parquet**

<center><img src="https://images2.imgbox.com/bd/ee/T5xzXTnY_o.png"></center> <!--db64-->

##### Ingesta del archivo "pit_stops.json"

1. Leer el archivo
2. Renombrar columnas y añadir nuevas columnas
3. Escribir datos en el contenedor **processed** del ADLS como **parquet**

<center><img src="https://i.postimg.cc/dV8W94f3/db65.png"></center>

##### Ingesta de los archivos "lap_times_split.csv"

1. Leer el directorio **lap_times** el cual contiene multiples archivos CSV
2. Renombrar columnas y añadir nuevas columnas
3. Escribir datos en el contenedor **processed** del ADLS como **parquet**

<center><img src="https://i.postimg.cc/ZntjGQ01/db66.png"></center>

##### Ingesta de los archivos "qualifying_split.json"

1. Leer el directorio **qualifying** el cual contiene multiples archivos Multi Line JSON
2. Renombrar columnas y añadir nuevas columnas
3. Escribir datos en el contenedor **processed** del ADLS como **parquet**

<center><img src="https://i.postimg.cc/pV94qxzr/db67.png"></center>

##### Ejecución de Todos los Notebooks

Para ejecutar todos los notebooks de manera eficiente, inicia el notebook `1_ingestar`. Esto permitirá que se lleve a cabo la ejecución en cadena de todos los notebooks asociados en un solo paso.


### **Paso 4 - Transformación de Datos**

#### Crear base de datos 

La base de datos `f1_presentation` almacenará los resultados finales, actuando como repositorio para consultas y análisis de datos posteriores.

#### Transformar los datos 

1. Renombrar columnas que no se ajustaron en la etapa de **Ingesta de Datos**.

2. Utilizando las funciones **Filter** y **Join**. Aplicaremos la función **Join** a las siguientes cinco tablas:

   <center><img src="https://i.postimg.cc/tC7fWR4B/db85.png"></center>

   **Campos a utilizar:**

   <center><img src="https://i.postimg.cc/7YRzX2FJ/db86.png"></center>

3. Utilizaremos las funciones **GroupBy** y **Agg** para realizar más transformaciones en los datos y generar un nuevo resultado en un archivo **parquet**.

4. También emplearemos la funcion **Window** para transformar los datos basándonos en los resultados obtenidos y generar un nuevo resultado en un archivo **parquet**.

### **Paso 5 - Análisis de Datos**

En este paso, hemos establecido la capa de **presentation**, diseñada específicamente para el análisis de datos. Esta capa servirá como un recurso fundamental para analistas y científicos de datos, permitiéndoles acceder a la información de manera eficiente y efectiva.

A través de la capa de presentation, los usuarios podrán realizar consultas SQL para extraer y manipular datos según sus necesidades específicas. Esto les permitirá obtener insights valiosos, realizar análisis detallados y generar informes que faciliten la toma de decisiones informadas.

### Paso 6: Caso propuesto para realizar una carga incremental

- **Ingesta incremental de datos**: Implementamos una ingesta manual, la cual abarcará datos de tres días, organizados en directorios que llevan la fecha en su nombre y serán cargados en el contenedor **raw** en Azure Data Lake Storage (ADLS). Se aplicará una carga incremental utilizando distintas estrategias para el proceso de ingesta de datos. El objetivo es ingerir datos de manera eficiente, minimizando los costos asociados con el reprocesamiento de datos que ya existen.

1. Vamos a suponer que los datos nos llegarán de forma diaria:

    *   El dia 21 de Marzo de 2021 nos llegó toda la historia de carreras hasta la **raceid** igual a **1047** (Estos fueron los datos cargados en el diseño anterior de Carga completa. Es decir, tenemos datos cargados hasta el 21 de Marzo de 2021)
    *   El dia 28 de Marzo de 2021 nos llegará solo la carrera de **raceid** igual a **1052**, que se realizó ese dia
    *   El dia 18 de Abril de 2021 nos llegará solo la carrera de **raceid** igual a **1053**, que se realizó ese dia
    *   Las carreras con **raceid** igual a **1048**, **1049** y **1050** no se realizaron. La carrera de **raceid** igual a **1051** se postergó  
    *   El próximo archivo nos llegará el día 2 de Mayo de 2021 y corresponde a la **raceid** igual a **1054**

<center><img src="https://i.postimg.cc/J7Bc7Pp7/db138.png"></center>

2. Vamos a analizar la data de los archivos que se encuentran en los tres directorios:

    *   Primero, indicar que todos los dias que se haga una carrera nos llegará el archivo. Este será un directorio que llevará la fecha
        de la carrera por nombre y contendrá 8 archivos:

<center><img src="https://i.postimg.cc/pX507K94/db140.png"></center>    

3. Los archivos **circuits**, **races**, **constructors** y **drivers** son iguales para todos, es decir, la data que se encuentra en esos 4 archivos se repite en los 3 directorios **2021-03-21**, **2021-03-28** y **2021-04-18**. 

4. Los archivos **results**, **pit_stops** y los directorios con archivos **lap_times** y **qualifying**, solo traeran data correspondiente a
la carrera en cuestión. Por ejemplo, para la fecha **2021-03-21** el archivo **results** traerá data correspondiente a toda la historia de carreras hasta la **raceid** igual a **1047**. No así para el archivo **2021-03-28** para el cual traerá solo data de la carrera realizada 
dicho dia. Lo mismo para el dia **2021-04-18**

5. Podemos ver en el archivo **races** las carreras programadas desde el 29 de Marzo de 2009 hasta el 12 de Diciembre de 2021. Ahi podemos verificar las fechas en que irán realizando las proximas carreras. También comentar que la carrera de **raceid** igual a **1047** se realizó el 13 de Diciembre de 2020. Posteriormente, se comenzó a utilizar Azure y Databricks y los datos desde la primera carrera hasta la realizada en esa fecha, fue enviada el dia 21 de Marzo de 2021. A partir de la carrera de **raceid** igual a **1052** se fue enviando la data el dia que se realizó la carrera.

<center><img src="https://i.postimg.cc/NfjNs12d/db139.png"></center>

6. Antes de crear un patrón de diseño para la carga de nuestros datos, teniamos el siguiente patrón de carga donde cada vez que llega un directorio nuevo cada dia VOLVEMOS A CARGAR TODO y SOBRESCRIBIMOS LO ANTERIOR:

Nos llega el directorio **2021-03-21** y lo procesamos
<center><img src="https://i.postimg.cc/fRjVTgtd/db141.png"></center>

Ya tenemos en nuestro repositorio **raw** el directorio **2021-03-21** y ahora nos llegó el directorio **2021-03-28**. Procesamos ambos, sobrescribiendo todo lo realizado en el proceso anterior, es decir, lo realizado para el directorio **2021-03-21**
<center><img src="https://i.postimg.cc/GpX9v8jh/db142.png"></center>

Ya tenemos en nuestro repositorio **raw** el directorio **2021-03-21** y el directorio **2021-03-28** y ahora nos llegó el directorio **2021-04-18**. Procesamos los tres, sobrescribiendo todo lo realizado en el proceso anterior, es decir, lo realizado para el directorio **2021-03-21** y para el directorio **2021-03-28**
<center><img src="https://i.postimg.cc/mDhkjLd3/db143.png"></center>

En resúmen, siempre volvemos a procesar TODO y NO SÓLO LOS DATOS NUEVOS QUE HAN LLEGADO. 

### Paso 7: Aplicar estrategias para realizar una carga incremental

- Vamos a aplicar la carga incremental solo a los archivos **circuits**, **races**, **constructors** y **drivers** que son los archivos de datos que son modificados segun el dia de carrera.

#### Eliminar las Bases de datos y tablas de "f1_proccesed" y "f1_presentation" y volver a crearlas

1. Comenzaremos vaciando el contenedor **raw** y cargando los directorios de los 3 dias, **2021-03-21**, **2021-03-28** y **2021-04-18**. La ruta para cada directorio vendria a ser de la siguiente forma:

    *   **/mnt/formula1dl/raw/2021-03-21**
    *   **/mnt/formula1dl/raw/2021-03-28**
    *   **/mnt/formula1dl/raw/2021-04-18**

#### Método 1: Utilizar parámetro de fecha para el directorio de archivos que llega diariamente

1. Modificar la **fecha de ingestión** utilizando el parámetro **p_file_date**
2. Modificar el **modo** de escritura, cambiamos de **overwrite** a **append**
3. Comenzar una carga incremental utilizando el parámetro **p-file_date** del notebook para los archivos **results** y **pit_stops** y los directorios con archivos **lap_times** y **qualifying**

#### Método 2: Utilizar parámetro de fecha para el directorio de archivos que llega diariamente y eliminación de particiones

#### Método 3: Utilizar parámetro de fecha para el directorio de archivos que llega diariamente y sobreescritura de particiones

#### Método de carga incremental para las transformaciones

