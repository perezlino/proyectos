###
 # @section Separación de recursos
 ##

#De manera similar a Hive, debemos tunear nuestro código SPARK indicando al menos:
#
# - spark.dynamicAllocation.maxExecutors: Máximo número de executors que nuestra aplicación puede solicitar a YARN
# - spark.executor.cores: Número de CPUs por executor, generalmente se le coloca 2.
# - spark.executor.memory: Cantidad de memoria RAM separada para cada executor, se le coloca como mínimo la suma total del peso de todos los archivos con los que se trabaja
# - spark.executor.memoryOverhead: Cantidad de memoria RAM adicional por si se llenase el "spark.executor.memory", generalmente se coloca el 10% de "spark.executor.memory" o 1gb
# - spark.driver.memory: Cantidad de memoria RAM asignada al driver, generalmente se le coloca 1gb
# - spark.driver.memoryOverhead: Cantidad de memoria RAM adicional por si se llenase el "spark.driver.memory", generalmente se coloca 1gb

#Instanciamos "spark" con el tuning
spark = SparkSession.builder.\
appName("Nombre de mi aplicacion").\
config("spark.driver.memory", "1g").\
config("spark.driver.memoryOverhead", "1g").\
config("spark.dynamicAllocation.maxExecutors", "10").\
config("spark.executor.cores", "2").\
config("spark.executor.memory", "2g").\
config("spark.executor.memoryOverhead", "1g").\
enableHiveSupport().\
getOrCreate()

'''
Como esto es una tecnologia de Big data, toda la parte teorica del tunning se va a aplicar
en este codigo, pero a diferencia de MapReduce, en Spark solamente tenemos que tunear un tipo
de contenedor especifico, los "EXECUTORS". Los EXECUTORS son esos contenedores que paralelizan
el proceso. Vamos a hablar solamente de los EXECUTORS y solo vamos a hablar de las siguientes 
lineas: 

                    config("spark.dynamicAllocation.maxExecutors", "10").\
                    config("spark.executor.cores", "2").\
                    config("spark.executor.memory", "2g").\
                    config("spark.executor.memoryOverhead", "1g").\

¿Cuál era el truco? Tomamos el archivo más pesado, probamos con 4 contenedores y le decimos:
"..si el archivo pesa 4 GB, significa que cada container debe separar 1 GB de RAM y el estándar 
de Batch dice que debo de colocar 2 CPUs..". ¿Cómo colocamos todo esto? por medio de estas 4
configuraciones en Spark. En "maxExecutors" estamos indicando el número de EXECUTORS, acá por
ejemplo son 10, mientras más containers executors tengamos, mas paralelizable el proceso. En
"executor.cores" estamos indicando el número de CPU para cada Executor. Ya sabemos que, el 
estándar en Batch es de 2. En "executor.memory" estamos colocando la memoria RAM para cada 
Executor, por ejemplo, si tenemos un archivo de 4G y son 4 Executors, seria un 1GB para cada 
Executor. Pero hay una variable más en Spark que debemos de considerar, recordemos de que
Spark va consumir muchisima memoria RAM y hay dos técnicas de TUNNING:

1.- Es indicar y configurar las tres primeras configuraciones que ya explicamos
2.- La cuarta configuración que corresponde al "memoryOverhead".

¿Qué significa "memoryOverhead"? ¿qué es lo que puede pasar? en el caso de Spark como todo vay a 
ir directamente a RAM, podría pasar lo siguiente, digamos que tú estás levantando 4 Executors para 
un archivo de 4 GB sería 1 GB por Executor. Pero podría pasar que el primer corte 1 GB, el segundo 
corte 1 GB, el tercer corte 900 MB y el cuarto corte 1.15 GB. El primer corte entra en un Executor, 
el segundo corte entra en otro Executor, el tercer corte entre en otro Executor y de hecho sobre 
100 MB de espacio, pero el último corte no va a entrar en el Executor, porque pesa 1.1 GB, a veces 
los cortes no son exactos. Nosotros solamente hemos reservado 1 GB de RAM, ¿de dónde va a salir la 
RAM del monto faltante de 0.1 GB que nos falta? ¿cuándo faltaba memoria RAM, qué pasaba en el Cluster? 
esa memoria RAM se emulaba de Disco duro y por lo tanto haces uso de la memoria virtual. ¿Qué 
implicaba hacer uso de la memoria virtual? como estamos haciendo uso de Disco duro, nuestro proceso va 
a ir hasta 100 veces más lento y nos vamos a aprovechar los beneficios de Spark. ¿Cómo podría solucionar 
esto? en Spark es clásico que ocurra este tipo de cosas, hemos tuneado 1 GB pero por ahí uno de los 
cortes pesa más, porque se llevó algunos registros más. Una manera simple podría ser, bueno sabemos que 
más o menos falla en + –10%, ¿qué significa esto? que si hemos configurado 4 Executors y el archivo 
pesa 4 GB, quiere decir que por ahí va a ver un corte que va a pesar un 10% más de lo que se supone 
debería pesar, es lo normal el motor de Spark fue programado así. Entonces uno podría decir: “…ah bueno 
si en el tuning me salió que debo solicitar 1 GB por executor, le voy a poner 1.1 GB, que es es el 
10%...” eso soluciona el problema, claro que sí, porque está dentro de lo que Spark tolera. El problema 
es que va a ser un desperdicio de memoria RAM. Para el primer Executor solo requeríamos de 1 GB y hemos 
separado 1.1 GB, estamos desperdiciando 0.1 GB. El segundo también requería solamente 1 GB, también 
estamos desperdiciando 0.1 GB. En el tercero se cargaron 900 MB, desperdicio de 0.2 GB y en el cuarto 
entró correctamente. En total hemos desperdiciado 0.4 GB. ¿Qué se hace en ese caso? para no separar de 
antemano esta memoria RAM adicional, ¿qué es lo que se hace? hay una zona de memoria RAM que se reserva 
de manera dinámica solamente para aquellos Executors que colapsen porque les falta memoria RAM, a esa 
zona se le llama “memoryOverhead” y es el 10% de lo que tu coloques en “memory”. Si acá por ejemplo en 
“executor.memory” nos salió 10 GB, tendríamos que colocar en “executor.memoryOverhead” el 10% de eso, que 
sería 1 GB. También hay una regla adicional más, si el 10% que nos salió es menor a 1 GB, también debemos 
de colocar 1 GB, por ejemplo, si yo en “executor.memory” colocamos 1 GB, el 10% de eso vendría a ser 
100 MB, 100 MB es menor a 1 GB, así que en “executor.memory” colocamos 1 GB. Para que no nos compliquemos 
en “executor.memory”  acá como mínimo siempre tiene que ir o bien 1 GB o bien el 10% lo que hemos separado 
en memoria RAM. 

Estos son los cuatro parámetros con los que debemos jugar para tunear nuestros códigos. Supongamos que 
nuestro proceso está yendo lento y queremos que vaya el doble de rápido, le ponemos el doble de Executors 
(en “maxExecutors”), en “executors.cores” siempre son 2, como hemos duplicado el número de Executors la 
memoria RAM por Executor va a la mitad (“executor.memory”) y el “executor.memoryOverhead” sería el 10% del 
“executor.memory” que se seria 100 MB, 100 MB es menor a 1 GB, por lo tanto se sigue quedando en 1 GB. Así 
es como se tunea.

Luego existen también algunos otros parámetros que son estándar, por ejemplo, en “appName” colocas el nombre 
que quieres que tenga tu aplicación y en "spark.driver.memory" y en "spark.driver.memoryOverhead" siempre 
deberás colocarle 1 GB. ¿Qué es esto del driver? es una zona de memoria RAM especial, en la cual nuestro 
programa se ejecuta en el Gateway. Básicamente es la zona de memoria que necesitan los lenguajes de programación 
(Python, Java, Scala o R) para funcionar, pero ya sabemos que eso está en el Gateway y en el Gateway no se 
ejecuta nada, por eso se coloca cantidades estándar, 1 GB. También es clásico, aunque las versiones de Spark 
actuales ya no lo requieren, dejar la función “enableHiveSupport()” que habilita la interconexión que hay entre 
Spark y Hive, para poder usar SparkSQL. Y finalmente con la función “getOrCreate()” creamos nuestra variable, 
podemos ponerle cualquier nombre que gustemos a la variable estándar, por estándar se recomienda colocar ese 
nombre como “spark”. Una vez que hemos decidido que número colocar en nuestro tunning, simplemente lo ejecutamos 
y listo, la variable “spark” se crea. 

'''