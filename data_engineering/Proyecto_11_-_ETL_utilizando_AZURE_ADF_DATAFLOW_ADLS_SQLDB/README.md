# Proyecto de Orquestación de Ingesta, Transformación y Carga de Datos con Azure Data Factory

[![p1098.png](https://i.postimg.cc/sfJm04kk/p1098.png)](https://postimg.cc/Wth0dgb8)

Este proyecto tiene como objetivo crear un flujo completo de **orquestación de datos** utilizando **Azure Data Factory (ADF)**, con actividades que abarcan desde la ingestión de datos hasta su transformación y carga en una base de datos, todo automatizado y coordinado mediante pipelines.

El proceso está dividido en varios pasos clave, que incluyen la creación de recursos de almacenamiento en **Azure Data Lake Storage (ADLS)** y **Azure Blob Storage (ABS)**, la configuración de bases de datos en **Azure SQL Database**, y la implementación de actividades de ingesta, transformación y carga de datos. Además, se contempla la automatización del flujo de trabajo mediante un pipeline de orquestación para gestionar la secuencia y dependencia de las actividades.

## Pasos principales del proyecto:

1. **Despliegue de Recursos en Azure**: Creación de cuentas de almacenamiento ADLS y ABS, configuración de bases de datos SQL y recursos de Azure Data Factory (ADF).
2. **Ingesta de Datos**: Extracción de datos desde diversas fuentes (HTTP, Blob Storage) hacia Azure Data Lake Storage, con pipelines parametrizados.
3. **Transformación de Datos**: Implementación de **Data Flows** para limpiar, enriquecer y transformar los datos.
4. **Carga de Datos**: Procesamiento final de los datos y carga en bases de datos SQL para su posterior análisis.
5. **Orquestación de Pipelines**: Coordinación y ejecución secuencial de pipelines de ingesta y transformación a través de Azure Data Factory.

## Despliegue del Proyecto

### Paso 1: Creación de cuentas de almacenamiento ADLS y ABS

Para comenzar, es esencial crear una cuenta de almacenamiento configurada para Azure Data Lake Storage (ADLS) y nombrarla `adlsproyectos`. Además, se debe crear otra cuenta de almacenamiento para Azure Blob Storage (ABS), que se llamará `absproyectos`. Una vez completadas estas configuraciones, procederemos a crear un contenedor en Azure Data Lake Storage llamado `raw` y un contenedor en Azure Blob Storage denominado `population`. A continuación, se presentan los pasos necesarios para llevar a cabo este proceso:

##### 1.1 Iniciar sesión en Azure
1. Ve al [Portal de Azure](https://portal.azure.com).
2. Inicia sesión con tu cuenta de Azure.

##### 1.2 Crear una cuenta de almacenamiento
1. En el menú del portal, selecciona **Crear un recurso**.
2. Busca **Almacenamiento** y selecciona **Cuenta de almacenamiento**.
3. Haz clic en **Crear**.

##### 1.3 Configurar la cuenta de almacenamiento
1. **Suscripción**: Selecciona la suscripción en la que deseas crear la cuenta.
2. **Grupo de recursos**: Puedes seleccionar un grupo de recursos existente o crear uno nuevo. Se creó el grupo de recursos **rg-proyectos**.
3. **Nombre de la cuenta de almacenamiento**: Proporciona un nombre único (debe ser entre 3 y 24 caracteres, y solo letras y números). Le di el nombre de **adlsproyectos**.
4. **Región**: Selecciona la región más cercana a tus usuarios o aplicaciones. Seleccione **East US**.
5. **Primary Service**: Selecciona **Azure Blob Storage or Azure Data Lake Storage Gen 2**.
6. **Performance**: Puedes elegir entre "Standard" o "Premium", según tus necesidades. Seleccione **Standard**.
7. **Replication**: Elige el tipo de replicación deseado (LRS, GRS, etc.). Seleccione **GRS**.

##### 1.4 Habilitar características de Data Lake
1. En la sección **Advanced**, asegúrate de habilitar **Habilitar el jerarquía de archivos (Hierarchical Namespace)** para usar características de ADLS Gen2.
2. Configura el resto de las opciones según tus necesidades (por ejemplo, cifrado, redes, etc.).

##### 1.5 Revisión y creación
1. Revisa todas las configuraciones.
2. Haz clic en **Crear** para crear la cuenta de almacenamiento.

##### 1.6 Acceder a tu Data Lake Storage
1. Una vez que se haya creado la cuenta, ve a **Cuentas de almacenamiento** en el menú del portal.
2. Selecciona la cuenta de almacenamiento que acabas de crear **adlsproyectos**.

##### 1.7 Crear los Contenedores 
1. En la página de la cuenta de almacenamiento, selecciona **"Contenedores"** en el menú lateral.
2. Haz clic en el botón **+ Contenedor** en la parte superior de la página.
3. Crea el primer contenedor:
   - Ingresa el nombre **`raw`**.
   - Establece el nivel de acceso como **Privado** para que no sea accesible públicamente.
   - Haz clic en **Crear**.
4. Crea el segundo contenedor:
   - Haz clic nuevamente en **+ Contenedor**.
   - Ingresa el nombre **`lookup`**.
   - Establece el nivel de acceso como **Privado**.
   - Haz clic en **Crear**.
   - Haz clic en el botón **Cargar** en la parte superior de la página.
   - Selecciona los archivos `country_lookup.csv` y `dim_date.csv` desde tu sistema local.
   - Haz clic en **Cargar** para subir los archivos al contenedor **lookup**.
6. Crea el tercer contenedor:
   - Haz clic nuevamente en **+ Contenedor**.
   - Ingresa el nombre **`processed`**.
   - Establece el nivel de acceso como **Privado**.
   - Haz clic en **Crear**.

##### 1.8 Crear una cuenta de almacenamiento para Azure Blob Storage
1. Repite el proceso de creación de cuenta de almacenamiento desde el paso 1.2.
2. Utiliza **absproyectos** como nombre para la cuenta de almacenamiento.
3. Configura las opciones de suscripción, grupo de recursos y región de la misma manera que hiciste para ADLS.
4. **Importante**: No habilites la opción **Habilitar el jerarquía de archivos (Hierarchical Namespace)**.

##### 1.9 Crear los Contenedores
1. Accede a la cuenta de almacenamiento **absproyectos** desde **Cuentas de almacenamiento**.
2. Selecciona "Contenedores" en el menú lateral.
3. Haz clic en el botón **+ Contenedor**.
4. En el cuadro de diálogo que aparece, ingresa el nombre `population`.
5. Le damos un de nivel de acceso  **Privado** para que no sea accesible públicamente.
6. Haz clic en **Crear**.
7. Subimos el archivo archivo `population_by_age.tsv.gz` al contenedor **population**.
8. Repetimos los pasos para crear el contenedor **configs** y subir el archivo `ecdc_file_list.json`.


### Paso 2: Creación de recurso Azure SQL Database 

##### 2.1 Crear un recurso de SQL Database
1. En el menú del portal, selecciona **Crear un recurso**.
2. Busca **SQL Database** y selecciona la opción.

##### 2.2 Configurar la base de datos
1. **Suscripción**: Selecciona la suscripción donde deseas crear la base de datos.
2. **Grupo de recursos**: Puedes seleccionar un grupo de recursos existente o crear uno nuevo. Selecciona **rg-proyectos** que creamos anteriormente.
3. **Nombre de la base de datos**: Escribe un nombre único para tu base de datos.
4. **Seleccionar el servidor**: 
   - Si ya tienes un servidor SQL creado, selecciona uno de la lista.
   - Si no tienes un servidor, haz clic en **Crear nuevo**.
     - Proporciona un nombre para el servidor.
     - Elige una ubicación (región) para el servidor.
     - Configura el nombre de usuario y la contraseña del administrador del servidor.

##### 2.3 Configurar el nivel de rendimiento
1. **Seleccionar un plan**: En la sección **Compute + storage**, elige el tipo de plan que deseas.
   - Puedes seleccionar entre opciones como **Basic**, **Standard** o **Premium** según tus necesidades.
2. **Configurar el almacenamiento**: Ajusta el almacenamiento según los requisitos de tu aplicación.

##### 2.4 Configuraciones adicionales (opcional)
1. **Opciones de configuración**: Configura las opciones adicionales según sea necesario, como el cifrado, la supervisión, etc.
2. **Firewall**: Asegúrate de configurar las reglas de firewall para permitir conexiones desde tu dirección IP o aplicaciones específicas.

##### 2.5 Revisión y creación
1. Revisa todas las configuraciones que has realizado.
2. Haz clic en **Crear** para iniciar el proceso de creación de la base de datos.

##### 2.6 Acceso a la base de datos
1. Una vez creada la base de datos, puedes acceder a ella desde el panel de Azure.
2. Para conectarte a la base de datos, utiliza herramientas como **SQL Server Management Studio** (SSMS) o **Azure Data Studio** con las credenciales de administrador que proporcionaste al crear el servidor.

##### 2.7 - Estructura de tablas que se deben crear
1. Se indica el scchema y las tablas que se deben crear

##### 2.8 - Utilizar `Azure Data Studio` para crear tablas


### **Paso 3: Crear el recurso de Azure Data Factory**

##### 3.1 Crear un nuevo recurso ADF

1. En el menú del portal, selecciona **Crear un recurso**.
2. Busca **Data Factory** y selecciona **Data Factory**.
3. Haz clic en **Crear**.

##### 3.2 Configurar ADF

1. **Seleccionar suscripción**: Escoge la suscripción de Azure que deseas utilizar.
2. **Grupo de recursos**: Selecciona **rg-proyectos** que creamos anteriormente.
3. **Nombre del Data Factory**: Proporciona un nombre único para tu Data Factory (debe tener entre 3 y 63 caracteres). Le di el nombre de **adf-proyecto2**.
4. **Región**: Selecciona la región donde deseas que se ubique el recurso. Seleccione **East US**.
5. **Versión**: Selecciona entre "V1" y "V2". Se recomienda utilizar la versión más reciente (V2) para acceder a las últimas características. Seleccione **V2**.

##### 3.3 Revisar y crear

1. **Revisión**: Verifica que toda la información ingresada sea correcta.
2. **Crear**: Haz clic en el botón **Crear** para provisionar el recurso.

##### 3.4 Acceder a Azure Data Factory

1. **Ir al recurso**: Una vez que la implementación se complete, puedes acceder a tu Data Factory haciendo clic en **Ir al recurso** o buscando el nombre de tu Data Factory en el menú de **Todos los recursos**.

### **Paso 4: Data Ingestion**

- Pasos a seguir en nuestra Ingesta de datos

    *   Crear Linked Services de origen y destino
    *   Crear datasets de origen y destino
    *   Crear un Pipeline de ingesta de datos  

[![p748.png](https://i.postimg.cc/nzpD9GPz/p748.png)](https://postimg.cc/wt4vSLH8)

- Pasos a seguir en nuestra Ingesta de datos desde HTTP hacia Azure Data Lake Storage Gen2

    *   Reutilizaremos contenedor en ADLS donde almacenar la data
    *   Crear un Linked Service de origen y reutilizar el Linked Service de destino
    *   Crear datasets parametrizados tanto de origen como destino 
    *   Crear un Pipeline de ingesta de datos parametrizado    

[![p859.png](https://i.postimg.cc/j24MhywM/p859.png)](https://postimg.cc/YLjNrGxF)
 
##### 4.1 Creación de Linked Services de origen y destino

1. Crear el Linked Service de origen **ls_ablob_covidreportingsa** que hace referencia a Azure Blob Storage
2. Crear el Linked Service de destino **ls_adls_covidreportingdl** que hace referencia a nuestro ADLS

##### 4.2 Creación de Datasets de origen y destino

1. Crear el Dataset de origen **ds_population_raw_gz** que hace referencia al archivo **population_by_age.tsv.gz** alojado en el contenedor **population** en Azure Blob Storage

2. Crear el Dataset de destino **ds_population_raw_tsv** que hace referencia al archivo **population_by_age.tsv** (que aún no existe, pero se creará de manera automática al ejecutar el pipeline) a nuestro ADLS, a la ruta **raw/population**. Siendo **raw** el contenedor y **population** el directorio

##### 4.3 Creación de un Pipeline para la ingesta de datos

1. Crear el Pipeline de ingesta de datos **pl_ingest_population_data** desde Azure Blob Storage hacia Azure Data Lake Storage Gen2, el cual contendrá una actividad **Copy data**

##### 4.4 Creación de Linked Service de origen

1. Crear el Linked Service de origen **ls_http_opendata_ecdc_europa_eu** que hace referencia a HTTP

##### 4.5 Creación de Datasets de origen y destino parametrizados

1. Crear el Dataset de origen **ds_ecdc_raw_csv_http** que hace referencia al archivo parametrizado **@dataset().relativeURL**. Dicho valor del parámetro será indicado al momento de ejecutar el pipeline

2. Crear el Dataset de destino **ds_ecdc_raw_csv_dl** que hace referencia al archivo parametrizado **@dataset().fileName** (que aún no existe, pero se creará de manera automática al ejecutar el pipeline) de nuestro ADLS, aque se almacenará en la ruta **raw/ecdc**. Siendo **raw** el contenedor y **ecdc** el directorio

##### 4.6 Creación de un Pipeline parametrizado 

1. Crear el Pipeline de ingesta de datos parametrizado **pl_ingest_ecdc_data** desde HTTP hacia Azure Data Lake Storage Gen2, el cual contendrá una actividad **Copy data**

##### 4.7 Modificaciones 

1. Parametrizar el Linked Service de origen **ls_http_opendata_ecdc_europa_eu**
2. Crear un nuevo parámetro en el dataset de origen **ds_ecdc_raw_csv_http**
3. Crear un nuevo parámetro en el pipeline **pl_ingest_ecdc_data**

##### 4.8 Modificación final al proceso de ingesta desde HTTP donde configuramos el pipeline para ingestar multiples archivos con solo una ejecución 

1. Creación del Dataset de origen `ds_ecdc_file_list` que hace referencia al archivo `ecdc_file_list.json` alojado en el contenedor `configs` en Azure Blob Storage

2. Actualizar el Pipeline de ingesta de datos parametrizado `pl_ingest_ecdc_data` el cual contendrá las actividades `Lookup`, `For Each` y `Copy data`

### **Paso 5: Data Transformation**

1. Pasos en la transformación de datos del archivo **cases_deaths.csv**
2. Pasos en la transformación de datos del archivo **hospital-admissions.csv**

##### 5.1 Creamos el Data flow "df_transform_cases_deaths" para transformar los datos del archivo "cases_deaths.csv"**

1. Crear un nuevo dataset de origen que haga referencia al archivo `cases_deaths.csv`, llamado  `ds_raw_cases_and_deaths`

2. Crear un nuevo dataset de origen que haga referencia al archivo `country_lookup.csv`, llamado  `ds_country_lookup`

3. Crear un nuevo dataset de origen que haga referencia a la ruta `processed/ecdc/cases_deaths`, llamado  `ds_processed_cases_and_deaths`

4. Crear un nuevo Data Flow llamado `df_transform_cases_deaths`

5. Crear el Pipeline de ingesta de datos `pl_process_cases_and_deaths_data` para ejecutar el Data flow recien creado

##### 5.2 Creamos el Data flow "df_transform_hospital_admissions" para transformar los datos del archivo "hospital-admissions.csv"

1. Crear un nuevo dataset de origen que haga referencia al archivo `hospital-admissions.csv`, llamado  `ds_raw_hospital_admission`

2. Crear un nuevo dataset de origen que haga referencia al archivo `dim_date.csv`, llamado  `ds_dim_date_lookup`

3. Crear un nuevo dataset de destino que haga referencia a la ruta `processed/ecdc/hospital_admissions_weekly`, llamado  `ds_processed_hospital_admissions_weekly`

4. Crear un nuevo dataset de destino que haga referencia a la ruta `processed/ecdc/hospital_admissions_daily`, llamado  `ds_processed_hospital_admissions_daily`

5. Crear un nuevo Data Flow llamado `df_transform_hospital_admissions`

6. Crear el Pipeline de ingesta de datos `pl_process_hospital_admissions_data` para ejecutar el Data flow recien creado

### **Paso 6: Data Loading**

1. Creación de un Linked Services con referencia hacia Azure SQL Database

2. Creación de Datasets de destino para cargar archivos obtenidos en la transformación de datos

3. Creación de Pipelines para cargar tablas de destino

### **Paso 7: Orquestación**

1. Orquestar todos los pipelines