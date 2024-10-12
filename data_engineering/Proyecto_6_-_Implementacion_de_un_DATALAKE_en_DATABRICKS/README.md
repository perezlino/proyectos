# Proyecto de implementación de una Arquitectura Data Lake en Databricks

[![p471.png](https://i.postimg.cc/02b90xJ9/p471.png)](https://postimg.cc/Tpv8Tz9s)

Este proyecto se centra en la implementación de un Data Lake utilizando Databricks, con el objetivo de almacenar, procesar y analizar datos de Fórmula 1. En nuestro diseño, recopilamos todos los datos del contenedor **raw**, los procesamos y actualizamos tanto el contenedor **processed** como el contenedor **presentation** en cada iteración. Esta arquitectura, que llamamos **Full Load**, es sencilla y resulta eficiente para gestionar pequeñas cantidades de datos.

## **Paso 1: Integración con Azure Data Lake Storage (ADLS)**

El primer paso consiste en la integración de Azure con Databricks. Esto incluye la configuración de un Azure Active Directory (AAD) Service Principal, que facilita la autenticación y el acceso seguro a ADLS. También se crea un Azure Key Vault para gestionar de forma segura las credenciales necesarias para acceder a los datos.

### Pasos Incluidos:
- Creación de un Service Principal en Azure para la autenticación.
- Configuración de un Key Vault para almacenar y gestionar secretos.
- Asignación de permisos adecuados para el Service Principal en Azure.
- Creación de un Secret Scope en Databricks para acceder a los secretos de forma segura.

## **Paso 2: Ingestión de Datos en el Data Lake**

Una vez configurada la integración, se procede a la ingesta de datos en el Data Lake. Se crean las bases de datos necesarias para organizar los datos crudos y procesados. A través de la carga manual de archivos desde ADLS, los datos se transforman y almacenan en formato Parquet, optimizando el espacio y el rendimiento.

### Pasos Incluidos:
- Creación de las bases de datos `f1_raw` y `f1_processed` en el Data Lake.
- Ingesta de archivos en diferentes formatos (CSV y JSON) desde ADLS.
- Aplicación de transformaciones a los datos para asegurar su calidad y consistencia.

## **Paso 3: Transformación de Datos**

En esta etapa, se realizan diversas transformaciones sobre los datos almacenados en el Data Lake. Se renombran columnas, se aplican funciones de unión y se utilizan agregaciones para generar nuevos conjuntos de datos que faciliten el análisis. También se implementan funciones de ventana para obtener métricas adicionales.

### Pasos Incluidos:
- Creación de la base de datos `f1_presentation` para almacenar los resultados finales del análisis.
- Transformación de datos utilizando técnicas avanzadas de SQL.
- Generación de archivos Parquet con los resultados procesados, listos para su análisis.

## **Paso 4: Análisis de Datos**

Finalmente, se establece la capa de **presentation**, que permite a los analistas y científicos de datos realizar consultas SQL y acceder a insights significativos de los datos almacenados en el Data Lake. Esta capa está diseñada para ser intuitiva, mejorando la eficiencia en el análisis y la generación de informes.

## Despliegue del Proyecto

### Paso 1: Integración con ADLS

Integrar Azure con Databricks implica varios pasos, desde la creación de un Azure Active Directory (AAD) Service Principal hasta la configuración de Databricks. Te muestro el paso a paso:

#### a) Crear un Azure Active Directory (AAD) Service Principal

1.- **Iniciar sesión en Azure Portal**:
   - Ve a [Azure Portal](https://portal.azure.com/) e inicia sesión con tu cuenta de Azure.

2.- **Crear un Service Principal**:
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

1.- **Crear un Key Vault**:
   - En el portal de Azure, selecciona **Create a resource**.
   - Busca y selecciona **Key Vault**.
   - Completa la información requerida:
     - **Name**: Elige un nombre único para el Key Vault.
     - **Subscription**: Selecciona tu suscripción.
     - **Resource group**: Selecciona un grupo de recursos existente o crea uno nuevo.
     - **Region**: Selecciona la región donde deseas crear el Key Vault.
   - Haz clic en **Review + Create** y luego en **Create**.

2.- **Agregar secretos al Key Vault**:
   - Ve al recurso del Key Vault que acabas de crear.
   - En el menú de la izquierda, selecciona **Secrets**.
   - Haz clic en **Generate/Import**.
   - Para cada secreto (Client ID, Tenant ID, Client Secret):
     - **Name**: Ingresa un nombre descriptivo (ej. `ClientID`, `TenantID`, `ClientSecret`).
     - **Value**: Introduce el password correspondiente de cada uno.
     - Haz clic en **Create**.

3.- **Copiar el Vault URI y el Resource ID**:
   - En la página principal de tu Key Vault, en el menú de la izquierda, selecciona **Properties**
   - Busca **Vault URI** y copialo.
   - Busca **Resource ID** y copialo.

#### c) Asignar permisos al Service Principal

1.- **Acceder a tu suscripción de Azure**:
   - Ve a **Subscriptions** en el portal de Azure.
   - Selecciona la suscripción a la que deseas acceder.

2.- **Asignar un rol al Service Principal**:
   - En la barra de herramientas, selecciona **Access control (IAM)**.
   - Haz clic en **Add role assignment**.
   - Selecciona un rol apropiado (por ejemplo, **Contributor** o **Databricks Admin**).
   - En **Assign access to**, selecciona **Azure AD user, group, or service principal**.
   - Busca tu Service Principal, selecciónalo y haz clic en **Save**.

#### d) Crear un Secret Scope en Databricks

1.- **Acceder al workspace de Databricks**:
   - Inicia sesión en tu workspace de Databricks.

2.- **Crear el Secret Scope**:
   - Haz clic en el símbolo de Databricks en la esquina superior izquierda.
   - En la barra de direcciones de tu navegador, agrega `#secrets/createScope` al final de la URL. Por ejemplo:
     ```
     https://<your-databricks-workspace>#secrets/createScope
     ```

3.- **Configurar el Secret Scope**:
   - Se abrirá la página para crear un nuevo Secret Scope.
   - **Nombre del Secret Scope**: Ingresa un nombre para tu Secret Scope (por ejemplo, `my_secret_scope`).
   - **Manage Principal**: Selecciona `All users`. Solo si tienes cuenta Premium podras seleccionar `Creator`
   - Para **Azure Key Vault** debemos indicar lo siguiente:
   - **DNS Name**: Pega el **Vault URI** que copiaste de Azure Key Vault.
   - **Resource ID**: Pega el **Resource ID** que también copiaste.

4.- **Crear el Secret Scope**:
   - Haz clic en **Create** para finalizar la creación del Secret Scope.

#### e) Acceder a los secretos en Databricks

1.- **Utilizar los secretos en un notebook**:
   - En un notebook, puedes acceder a los secretos utilizando el siguiente código:

     ```python
     client_id = dbutils.secrets.get(scope="my_secret_scope", key="ClientID")
     tenant_id = dbutils.secrets.get(scope="my_secret_scope", key="TenantID")
     client_secret = dbutils.secrets.get(scope="my_secret_scope", key="ClientSecret")
     ```
   - **scope**: Corresponde al nombre de tu Databricks Secret Scope
   - **key**: Corresponde al nombre del secreto que le diste al `ClienteID`, `TenantID` y `ClientSecret` en Azure Key Vault.

#### f) Probar la integración

1.- **Realizar una prueba**:
   - Ejecuta un comando en tu notebook que requiera acceso a recursos de Azure, utilizando los secretos que has recuperado.

#### Consideraciones adicionales

- Asegúrate de que el Service Principal tenga acceso al Key Vault configurado.
- Utilizar Databricks Scoped Secrets es una excelente manera de manejar credenciales de forma segura.

### Paso 2: Ingestión

1. **Crear bases de datos**: Comienza el proceso estableciendo la base de datos `f1_raw`, que servirá para almacenar los datos en bruto. Luego, crea la base de datos `f1_processed`, que será el destino para los datos ingeridos durante el proceso de transformación. 

2. **Ingesta manual de datos**: Procederemos a realizar la ingesta manual de datos, seleccionando y cargando archivos de diversos formatos desde el contenedor raw en Azure Data Lake Storage (ADLS).

<center><img src="https://images2.imgbox.com/a1/9f/t0LTa2k6_o.png"></center> <!--db61-->

3. **Procesar y guardar resultados**: Aplicar transformaciones a los archivos ingeridos y guardar los resultados en la base de datos `f1_processed`, que servirá como fuente para el siguiente paso del proceso.

#### a) Crear base de datos

La base de datos **f1_raw** se creará en la ruta `/mnt/formula1dl/raw`. Es importante mencionar que al momento de crear la base de datos la ruta `/mnt/formula1dl/raw` se crea automaticamente.
```sql
CREATE DATABASE IF NOT EXISTS f1_raw
LOCATION "/mnt/formula1dl/raw"
```

La base de datos **f1_processed** se creará en la ruta `/mnt/formula1dl/processed`. Es importante mencionar que al momento de crear la base de datos la ruta `/mnt/formula1dl/processed` se crea automaticamente.
```sql
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dl/processed"
```

#### b) Carga de archivos hacia la ruta de ingesta: 

En Databricks, cuando utilizas la interfaz de usuario para cargar archivos, a menudo se limita la ubicación de destino a directorios predeterminados, como `/FileStore/tables/`. Sin embargo, si deseas cargar archivos directamente a una ubicación específica como `/mnt`, puedes hacerlo a través de otros métodos. Aquí tienes cómo hacerlo:

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

#### c) Detalles de la Ingesta de Archivos

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

#### d) Ejecución de Todos los Notebooks

Para ejecutar todos los notebooks de manera eficiente, inicia el notebook `1_ingestar`. Esto permitirá que se lleve a cabo la ejecución en cadena de todos los notebooks asociados en un solo paso.

### **Paso 3 - Transformación de Datos**

1. **Crear bases de datos**: La base de datos `f1_presentation` almacenará los resultados finales, actuando como repositorio para consultas y análisis de datos posteriores.

La base de datos **f1_presentation** se creará en la ruta `/mnt/formula1dl/presentation`. Es importante mencionar que al momento de crear la base de datos la ruta `/mnt/formula1dl/presentation` se crea automaticamente.
```sql
CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1dl/presentation"
```

2. **Renombrar columnas** que no se ajustaron en la etapa de **Ingesta de Datos**.

3. **Transformar los datos** utilizando las funciones **Filter** y **Join**. Aplicaremos la función **Join** a las siguientes cinco tablas:

   <center><img src="https://i.postimg.cc/tC7fWR4B/db85.png"></center>

   **Campos a utilizar:**

   <center><img src="https://i.postimg.cc/7YRzX2FJ/db86.png"></center>

4. A partir del resultado del paso 2, utilizaremos las funciones **GroupBy** y **Agg** para realizar más transformaciones en los datos y generar un nuevo resultado en un archivo **parquet**.

5. También emplearemos las funciones **Window** para transformar los datos basándonos en los resultados obtenidos en el paso 2  y generar un nuevo resultado en un archivo **parquet**.

### **Paso 4 - Análisis de Datos**

En este paso, hemos establecido la capa de **presentation**, diseñada específicamente para el análisis de datos. Esta capa servirá como un recurso fundamental para analistas y científicos de datos, permitiéndoles acceder a la información de manera eficiente y efectiva.

A través de la capa de presentation, los usuarios podrán realizar consultas SQL para extraer y manipular datos según sus necesidades específicas. Esto les permitirá obtener insights valiosos, realizar análisis detallados y generar informes que faciliten la toma de decisiones informadas.
