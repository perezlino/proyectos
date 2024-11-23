# Implementación Completa de Proyecto de Análisis de Datos con Azure Synapse, Azure Data Lake y Power BI

[![p1257.png](https://i.postimg.cc/prVpjcSR/p1257.png)](https://postimg.cc/YGVrKRks)

Este proyecto tiene como objetivo un proceso de análisis de datos completo, desde la ingestión hasta la visualización en dashboards interactivos, utilizando tecnologías avanzadas en la nube de Azure, como **Azure Data Lake Storage (ADLS)**, **Azure Synapse Analytics**, y **Power BI** para manejar grandes volúmenes de datos de manera eficiente.

## **Pasos principales del proyecto**

1. **Creación de la cuenta de almacenamiento y contenedores**: Configura una cuenta de almacenamiento Azure Data Lake Storage Gen2 llamada `adlsproyectos` y crea los contenedores `proyectosasa`, `raw`, `refined` y `processed` para organizar los datos.

2. **Configuración de Azure Synapse Analytics**: Provisiona un workspace de Azure Synapse Analytics e integra el almacenamiento `adlsproyectos` para realizar análisis de datos a gran escala.

3. **Ingesta de datos**: Carga los datos en bruto en el contenedor `raw` de Azure Data Lake Storage desde una fuente externa.

4. **Exploración y creación de External Tables**: Explora los datos iniciales y crea tablas externas en Synapse para facilitar el acceso y análisis de los datos.

5. **Transformación de datos**: Transforma los datos usando PySpark en Azure Synapse para realizar optimizaciones y adaptaciones necesarias para el análisis.

6. **Visualización de datos**: Visualiza los resultados en Power BI mediante tablas optimizadas en Dedicated SQL Pool en Azure Synapse.

## **Tecnologías Utilizadas**

- **Azure Data Lake Storage Gen2 (ADLS Gen2)**: Almacenamiento escalable y optimizado para grandes volúmenes de datos, utilizado para almacenar los datos en sus distintas etapas (`raw`, `refined`, `processed`).
- **Azure Synapse Analytics**: Plataforma de análisis integrada para la ingesta, procesamiento y visualización de datos a gran escala, que incluye **SQL Pools**, **Spark Pools** y **Pipelines**.
- **PySpark**: Framework de procesamiento distribuido de datos basado en Apache Spark, utilizado para transformar y procesar datos en el entorno de Azure Synapse.
- **SQL Server**: Utilizado a través de **Dedicated SQL Pool** para almacenar datos procesados y optimizados.
- **Power BI**: Herramienta de visualización de datos para la creación de dashboards interactivos y reportes a partir de los datos cargados en Azure Synapse.


## Despliegue del Proyecto

### Paso 1: Creación ADLS "adlsproyectos" y contenedores

Para comenzar, es esencial crear una cuenta de almacenamiento configurada para Azure Data Lake Storage (ADLS) y nombrarla `adlsproyectos`. Una vez completado esta configuración, procederemos a crear cuatro contenedores en Azure Data Lake Storage llamados `proyectosasa`, `raw`, `refined` y `processed`. A continuación, se presentan los pasos necesarios para llevar a cabo este proceso:

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
   - Ingresa el nombre **`proyectosasa`**.
   - Establece el nivel de acceso como **Privado** para que no sea accesible públicamente.
   - Haz clic en **Crear**.
4. Crea el segundo contenedor:
   - Haz clic nuevamente en **+ Contenedor**.
   - Ingresa el nombre **`raw`**.
   - Establece el nivel de acceso como **Privado**.
   - Haz clic en **Crear**.
5. Crea el tercer contenedor:
   - Haz clic nuevamente en **+ Contenedor**.
   - Ingresa el nombre **`refined`**.
   - Establece el nivel de acceso como **Privado**.
   - Haz clic en **Crear**.
6. Crea el tercer contenedor:
   - Haz clic nuevamente en **+ Contenedor**.
   - Ingresa el nombre **`processed`**.
   - Establece el nivel de acceso como **Privado**.
   - Haz clic en **Crear**.   

### **Paso 2: Crear el recurso de Azure Synapse Analytics**

##### 2.1 Crear un nuevo recurso ASA

1. En el menú del portal, selecciona **Crear un recurso**.
2. Busca **Azure Synapse Analytics** y en Marketplace selecciona **Azure Synapse Analytics**.
3. Haz clic en **Crear**.

##### 2.2 Configurar ASA

1. **Seleccionar suscripción**: Escoge la suscripción de Azure que deseas utilizar.
2. **Grupo de recursos**: Selecciona **rg-proyectos** que creamos anteriormente.
3. **Managed resource group**: este es un contenedor que contiene todos los recursos auxiliares creados por Synapse Analytics para tu workspace. En caso de que no proporciones un nombre para el **Managed resource group**, Synapse creará su propio **Managed resource group**. Si tienes políticas estrictas a nivel organizacional, puedes mencionar el nombre de acuerdo a esas políticas, si no, puedes dejarlo en blanco y Synapse creará el suyo propio. Le dejaremos en blanco.
4. **Nombre del Workspace**: Le di el nombre de **asa-proyectos**.
5. **Región**: Selecciona la región donde deseas que se ubique el recurso. Seleccione **East US 2**. (En mi suscripción de Azure Student no me permitió crear en la región **East US**)
6. **Seleccionar un Data Lake Storage Gen2**: Seleccionamos la cuenta de almacenamiento **adlsproyectos** y el contenedor **proyectosasa**. Tanto la cuenta de almacenamiento como el contenedor se convertiran en servicios **principales** del workspace.

##### 2.3 Configurar ASA: Security

1. **Método de autenticación**: Aquí nos pregunta si deseamos usar `solo la autenticación de Azure AD (Azure Active Directory)` o `usar tanto la autenticación local como la de Azure Active Directory`. Esto es porque tenemos algo llamado **Serverless SQL Pool** que en el backend es una **SQL Database** o un **SQL Server**. Seleccionamos `Use both local and Microsoft Entra ID authentication`.
2. **SQL Server admin login** y **SQL Password**: Necesitamos proporcionar un nombre de usuario y una contraseña. Vamos a configurar `sqladmin` y establecer una contraseña que será `Admin1234`. No vamos a cambiar ninguna de las otras opciones.

##### 2.4 - Configurar ASA: Otras opciones de configuración

Pasemos a la siguiente sección, **Networking**. No vamos a crear ninguna red administrada, así que hacemos clic en siguiente. No añadiremos **Tags**, así que hacemos clic en **Review + create**.

##### 2.5 Revisar y crear

1. **Revisión**: Verifica que toda la información ingresada sea correcta.
2. **Crear**: Haz clic en el botón **Crear** para provisionar el recurso.

##### 2.6 - Acceder a Azure Synapse Analytics

1. **Ir al recurso**: Una vez que la implementación se complete, puedes acceder a tu Azure Synapse Analytics haciendo clic en **Ir al recurso** o buscando el nombre de tu Azure Synapse Analytics en el menú de **Todos los recursos**.


### **Paso 3: Data Ingestion**

##### 3.1 Carga de datos

1. Carga de datos en el contenedor `raw` ubicado en el Azure Data Lake `adlsproyectos`


### **Paso 4: Data Exploration**

##### 4.1 Exploración inicial de datos y creación de una EXTERNAL TABLE

1. Exploración inicial del archivo `Unemployment.csv` y creación de una **EXTERNAL TABLE** en el contenedor `refined`


### **Paso 5: Data Transformation**

##### 5.1 Transformación de datos

1. Transformación de datos utilizando `PySpark` y `Spark Pool`


### **Paso 6: Preparación y visualización de datos**

1. Creación y carga de datos en `Dedicated SQL Pool`
2. Creación de reporte en `Power BI` conectado a Dedicated SQL Pool de y publicarlo en `Power BI Service`
