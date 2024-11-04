# Proyecto de ingesta y carga de datos desde Azure Datalake Storage hacia Azure SQL Database utilizando Azure Data Factory

[![p726.png](https://i.postimg.cc/Znd7207d/p726.png)](https://postimg.cc/QKjJWNr8)

Este proyecto tiene como objetivo implementar un flujo completo de ingesta y carga de datos utilizando Azure Data Lake Storage (ADLS) y Azure Data Factory. Se centra en la ingesta de archivos CSV almacenados en un repositorio de GitHub, su transformación en formato JSON y su carga en una base de datos SQL en Azure.


## Despliegue del Proyecto

### Paso 1: Creación de cuenta de almacenamiento ADLS y contenedor 

Antes de proceder con este paso, es necesario crear una cuenta de almacenamiento configurada para Azure Data Lake Storage (ADLS) y nombrarla `adlsproyectos`. Una vez completada esta tarea, crearemos un contenedor en Azure Data Lake Storage: `raw`. A continuación, se detallan los pasos necesarios para llevar a cabo este proceso:

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

##### 1.7 Crear el Contenedor "raw"
1. En la página de la cuenta de almacenamiento, busca y selecciona "Contenedores" en el menú lateral.
2. Haz clic en el botón **+ Contenedor** en la parte superior de la página.
3. En el cuadro de diálogo que aparece, ingresa el nombre `raw`.
4. Le damos un de nivel de acceso  **Privado** para que no sea accesible públicamente.
5. Haz clic en **Crear**.

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

### **Paso 3: Crear el recurso de Azure Data Factory**

##### 3.1 Crear un nuevo recurso ADF

1. En el menú del portal, selecciona **Crear un recurso**.
2. Busca **Data Factory** y selecciona **Data Factory**.
3. Haz clic en **Crear**.

##### 3.2 Configurar ADF

1. **Seleccionar suscripción**: Escoge la suscripción de Azure que deseas utilizar.
2. **Grupo de recursos**: Selecciona **rg-proyectos** que creamos anteriormente.
3. **Nombre del Data Factory**: Proporciona un nombre único para tu Data Factory (debe tener entre 3 y 63 caracteres). Le di el nombre de **adf-proyecto1**.
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
 
##### 4.1 Creación de Linked Services de origen y destino

1. Crear el Linked Service de origen **LS_HttpServer_GitHub** que hace referencia al repositorio Github
2. Crear el Linked Service de destino **LS_AzureDataLakeStorageG2** que hace referencia a nuestro ADLS

##### 4.2 Creación de Datasets de origen y destino para copiar el archivo "ipl 2008.csv"

1. Crear el Dataset de origen **ds_Source_IPL_2008_csv** que hará referencia al archivo **ipl 2008.csv** del repositorio Github
2. Crear el Dataset de destino **ds_Destination_IPL_2008_json** que hará referencia al archivo **IPL_2008.json** que no existe y que se almacenará en el contenedor **raw**

##### 4.3 Creación de Pipeline "pl_copy_IPL_DATA_2008"

1. Crear el Pipeline de ingesta de datos **pl_copy_IPL_DATA_2008**, el cual contendrá una actividad **Copy data** y este copiará el archivo **ipl 2008.csv** ubicado en el repositorio Github, en la ruta ADLS **raw/IPL_2008.json**, almacenandolo en formato **JSON**
   
##### 4.4 Creación de Pipeline "pl_Copy_FULL_IPL_DATA"

1.  Una forma mucho más eficiente de trabajar con múltiples archivos es utilizando **PARÁMETROS**. Para ello trabajaremos solo con 2 datasets, uno de origen y de destino y crearemos dos parámetros, ambos a nivel de dataset, uno en cada dataset. Reutilizaremos los datasets **ds_Source_IPL_2008_csv** y **ds_Destination_IPL_2008_json** los cuales haran referencia a un parámetro y no a un archivo. Modificaremos sus nombres:

    *   **ds_Source_IPL_csv**
    *   **ds_Destination_IPL_json**

    Crearemos el pipeline **pl_Copy_FULL_IPL_DATA** el cual utilizará las actividades **Lookup** y **For Each**    

### **Paso 5: Data Loading**

1. Pasos a seguir en nuestra Ingesta de datos

    *   Crear Linked Service con referencia hacia Azure SQL Database
    *   Crear datasets de origen y destino
    *   Crear un Pipeline de carga de datos    

##### 5.1 Creación de un Linked Services con referencia hacia Azure SQL Database

1. Crear el Linked Service de destino **LS_AzureSqlDatabase_IPL_db** que hace referencia hacia Azure SQL Database. 

##### 5.2 Creación de Datasets de origen y destino para cargar el archivo "ipl 2008.json"

1. Crear el Dataset de origen **ds_Valid_IPL_Data_Json** que hará referencia al archivo **ipl 2008.json** que se encuentra en nuestro ADLS
2. Crear el Dataset de destino **ds_AzureSqlTable_IPL_Data** que hará referencia a la tabla **dbo.tbl_IPLData** en Azure SQL Database

##### 5.3 Creación de Pipeline "pl_Copy_Valid_IPL_To_SQL"

1. Creación y configuración de un nuevo Pipeline que se llamará **pl_Copy_Valid_IPL_To_SQL** que nos permitirá realizar la carga del archivo **ipl 2008.json** en la tabla **dbo.tbl_IPLData**. Agregaremos la actividad **Copy Data**

##### 5.4 Creación de Pipeline "pl_Copy_Full_IPL_Data_using_DF"

1. Cargar todos los archivos JSON del contenedor "raw" de nuestro ADLS utilizando Data Flows. Para ello creamos un nuevo Pipeline llamado **pl_Copy_Full_IPL_Data_using_DF** en el cual ejecutaremos nuestro Data Flow

    Reutilziaremos el dataset de origen **ds_Valid_IPL_Data_Json** y el dataset de destino **ds_AzureSqlTable_IPL_Data**

