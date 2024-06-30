//Librerías

//Importamos la instancia SparkSession para interactuar con Spark
import org.apache.spark.sql.SparkSession

//Objetos para definir la metadata
//import org.apache.spark.sql.types.{StructType, StructField}

//Importamos los tipos de datos que usaremos
//import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}

//Podemos importar todos los utilitarios con la siguiente sentencia
import org.apache.spark.sql.types._

//Importamos todos los objetos utilitarios dentro de una variable
//import org.apache.spark.sql.{functions => f}

//Importamos las librerías para implementar UDFs
import org.apache.spark.sql.functions.udf

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-scala-etl")
      .master("spark://spark-master:7077")
      .config("spark.version", "2.4.5")
      .getOrCreate()

    //////////////////////////////// Lectura de datos //////////////////////////////// 

    //Leemos los datos de transacciones
    var dfJson = spark.read.format("json").load("hdfs:///user/proyectos/proyecto_3/input/transacciones_bancarias.json")

    //Vemos el esquema (para el archivo JSON no es necesario crearle un schema de tipo de dato)
    dfJson.printSchema()

    //Mostramos los datos
    dfJson.show()


    //Leemos el archivo de riesgo crediticio
    var dfRiesgo = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(
        StructType(
            Array(
              StructField("ID_CLIENTE", StringType, true),
              StructField("RIESGO_CENTRAL_1", DoubleType, true),
              StructField("RIESGO_CENTRAL_2", DoubleType, true),
              StructField("RIESGO_CENTRAL_3", DoubleType, true)
            )
        )
    ).load("hdfs:///user/proyectos/proyecto_3/input/RIESGO_CREDITICIO.csv")

    //Vemos el esquema
    dfRiesgo.printSchema()

    //Mostramos los datos
    dfRiesgo.show()


    //////////////////////////////// Modelamiento //////////////////////////////// 

    //Estructuramos dfEmpresa

    //Seleccionamos los campos
    var dfEmpresa = dfJson.select(
      dfJson.col("EMPRESA.ID_EMPRESA").alias("ID_EMPRESA"),
      dfJson.col("EMPRESA.NOMBRE_EMPRESA").alias("NOMBRE_EMPRESA")
    ).distinct()

    //Vemos el esquema
    dfEmpresa.printSchema()

    //Contamos los registros
    println(dfEmpresa.count())

    //Mostramos los datos
    dfEmpresa.show()


    //Estructuramos dfPersona

    //Seleccionamos los campos
    var dfPersona = dfJson.select(
      dfJson.col("PERSONA.ID_PERSONA").alias("ID_PERSONA"),
      dfJson.col("PERSONA.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
      dfJson.col("PERSONA.EDAD").alias("EDAD"),
      dfJson.col("PERSONA.SALARIO").alias("SALARIO")
    ).distinct()

    //Vemos el esquema
    dfPersona.printSchema()

    //Contamos los registros
    println(dfPersona.count())

    //Mostramos los datos
    dfPersona.show()


    //Estructuramos dfTransaccion

    //Seleccionamos los campos
    var dfTransaccion = dfJson.select(
      dfJson.col("PERSONA.ID_PERSONA").alias("ID_PERSONA"),
      dfJson.col("EMPRESA.ID_EMPRESA").alias("ID_EMPRESA"),
      dfJson.col("TRANSACCION.MONTO").alias("MONTO"),
      dfJson.col("TRANSACCION.FECHA").alias("FECHA")
    )

    //Vemos el esquema
    dfTransaccion.printSchema()

    //Contamos los registros
    println(dfTransaccion.count())

    //Mostramos los datos
    dfTransaccion.show()


    //////////////////////////////// Reglas de calidad //////////////////////////////// 

    //Aplicamos las reglas de calidad a dfTransaccion
    var dfTransaccionLimpio = dfTransaccion.filter(
      (dfTransaccion.col("ID_EMPRESA").isNotNull) &&
      (dfTransaccion.col("ID_EMPRESA").isNotNull) &&
      (dfTransaccion.col("MONTO").isNotNull) &&
      (dfTransaccion.col("FECHA").isNotNull) &&
      (dfTransaccion.col("MONTO") > 0)
    )

    //Vemos el esquema
    dfTransaccionLimpio.printSchema()

    //Mostramos los datos
    dfTransaccionLimpio.show()


    //Aplicamos las reglas de calidad a dfPersona
    var dfPersonaLimpio = dfPersona.filter(
      (dfPersona.col("ID_PERSONA").isNotNull) &&
      (dfPersona.col("NOMBRE_PERSONA").isNotNull) &&
      (dfPersona.col("SALARIO").isNotNull) &&
      (dfPersona.col("SALARIO") > 0)
    )

    //Vemos el esquema
    dfPersonaLimpio.printSchema()

    //Mostramos los datos
    dfPersonaLimpio.show()


    //Aplicamos las reglas de calidad a dfEmpresa
    var dfEmpresaLimpio = dfEmpresa.filter(
      (dfEmpresa.col("ID_EMPRESA").isNotNull) && 
      (dfEmpresa.col("NOMBRE_EMPRESA").isNotNull)
    )

    //Vemos el esquema
    dfEmpresaLimpio.printSchema()

    //Mostramos los datos
    dfEmpresaLimpio.show()


    //Aplicamos las reglas de calidad a dfRiesgo
    var dfRiesgoLimpio = dfRiesgo.filter(
      (dfRiesgo.col("ID_CLIENTE").isNotNull) &&
      (dfRiesgo.col("RIESGO_CENTRAL_1").isNotNull) &&
      (dfRiesgo.col("RIESGO_CENTRAL_2").isNotNull) &&
      (dfRiesgo.col("RIESGO_CENTRAL_3").isNotNull) &&
      (dfRiesgo.col("RIESGO_CENTRAL_1") >= 0) &&
      (dfRiesgo.col("RIESGO_CENTRAL_2") >= 0) &&
      (dfRiesgo.col("RIESGO_CENTRAL_3") >= 0)
      
    )

    //Vemos el esquema
    dfRiesgoLimpio.printSchema()

    //Mostramos los datos
    dfRiesgoLimpio.show()


    //////////////////////////////// Preparación de tablones //////////////////////////////// 

    //PASO 1 (<<T_P>>): AGREGAR LOS DATOS DE LAS PERSONAS QUE REALIZARON LAS TRANSACCIONES
    var df1 = dfTransaccionLimpio.join(
      dfPersonaLimpio,
      dfTransaccionLimpio.col("ID_PERSONA") === dfPersonaLimpio.col("ID_PERSONA"),
      "inner"
    ).select(
      dfPersonaLimpio.col("ID_PERSONA"),
      dfPersonaLimpio.col("NOMBRE_PERSONA"),
      dfPersonaLimpio.col("EDAD").alias("EDAD_PERSONA"),
      dfPersonaLimpio.col("SALARIO").alias("SALARIO_PERSONA"),
      dfTransaccionLimpio.col("ID_EMPRESA"),
      dfTransaccionLimpio.col("MONTO").alias("MONTO_TRANSACCION"),
      dfTransaccionLimpio.col("FECHA").alias("FECHA_TRANSACCION")
    )

    //Mostramos los datos
    df1.show()


    //PASO 2 (<<T_P_E>>): AGREGAR LOS DATOS DE LAS EMPRESAS EN DONDE SE REALIZARON LAS TRANSACCIONES
    var df2 = df1.join(
      dfEmpresaLimpio,
      df1.col("ID_EMPRESA") === dfEmpresaLimpio.col("ID_EMPRESA"),
      "inner"
    ).select(
      df1.col("ID_PERSONA"),
      df1.col("NOMBRE_PERSONA"),
      df1.col("EDAD_PERSONA"),
      df1.col("SALARIO_PERSONA"),
      df1.col("ID_EMPRESA"),
      dfEmpresaLimpio.col("NOMBRE_EMPRESA"),
      df1.col("MONTO_TRANSACCION"),
      df1.col("FECHA_TRANSACCION")
    )

    //Mostramos los datos
    df2.show()


    //PASO 3 (<<RIESGO PONDERADO>>): OBTENEMOS EL RIESGO CREDITICIO PONDERADO (1*R_1 + 2*R_2 + 1*R_3) / 4

    //Implementamos la función
    def calcularRiesgoPonderado(riesgoCentral1 : Double, riesgoCentral2 : Double, riesgoCentral3 : Double) : Double = {
      var resultado : Double = 0.0

      resultado = (1*riesgoCentral1 + 2*riesgoCentral2 + 1*riesgoCentral3) / 4

      return resultado
    }

    //Creamos la función personalizada
    var udfCalcularRiesgoPonderado = udf(
      (
        riesgoCentral1 : Double, 
        riesgoCentral2 : Double,
        riesgoCentral3 : Double
      ) => calcularRiesgoPonderado(
        riesgoCentral1, 
        riesgoCentral2,
        riesgoCentral3
      )
    )

    //Registramos el UDF
    spark.udf.register("udfCalcularRiesgoPonderado", udfCalcularRiesgoPonderado)


    //Aplicamos la función
    var df3 = dfRiesgoLimpio.select(
      dfRiesgoLimpio.col("ID_CLIENTE").alias("ID_CLIENTE"),
      udfCalcularRiesgoPonderado(
          dfRiesgoLimpio.col("RIESGO_CENTRAL_1"),
          dfRiesgoLimpio.col("RIESGO_CENTRAL_2"),
          dfRiesgoLimpio.col("RIESGO_CENTRAL_3"),
      ).alias("RIESGO_PONDERADO")
    )

    //Mostramos los datos
    df3.show()

    
    //PASO 4 (<<T_P_E_R>>): Agregamos el riesgo crediticio ponderado a cada persona que realizo la transacción
    var dfTablon = df2.join(
      df3,
      df2.col("ID_PERSONA") === df3.col("ID_CLIENTE"),
      "inner"
    ).select(
      df2.col("ID_PERSONA"),
      df2.col("NOMBRE_PERSONA"),
      df2.col("EDAD_PERSONA"),
      df2.col("SALARIO_PERSONA"),
      df2.col("ID_EMPRESA"),
      df2.col("NOMBRE_EMPRESA"),
      df2.col("MONTO_TRANSACCION"),
      df2.col("FECHA_TRANSACCION"),
      df3.col("RIESGO_PONDERADO")
    )

    //Mostramos los datos
    dfTablon.show()


    //////////////////////////////// Procesamiento //////////////////////////////// 
    //REPORTE 1:
    //
    // - TRANSACCIONES MAYORES A 500 DÓLARES
    // - CON RIESGO CREDITICIO PONDERADO MAYOR A 0.5
    // - REALIZADAS EN AMAZON
    // - POR PERSONAS ENTRE 30 A 39 AÑOS
    // - CON UN SALARIO DE 1000 A 5000 DOLARES
    //
    //REPORTE 2:
    //
    // - TRANSACCIONES MAYORES A 500 DÓLARES
    // - CON RIESGO CREDITICIO PONDERADO MAYOR A 0.5
    // - REALIZADAS EN AMAZON
    // - POR PERSONAS ENTRE 40 A 49 AÑOS
    // - CON UN SALARIO DE 2500 A 7000 DOLARES
    //
    //REPORTE 3:
    //
    // - TRANSACCIONES MAYORES A 500 DÓLARES
    // - CON RIESGO CREDITICIO PONDERADO MAYOR A 0.5
    // - REALIZADAS EN AMAZON
    // - POR PERSONAS ENTRE 50 A 60 AÑOS
    // - CON UN SALARIO DE 3500 A 10000 DOLARES
    //
    //Notamos que todos los reportes comparten:
    // - TRANSACCIONES MAYORES A 500 DÓLARES
    // - CON RIESGO CREDITICIO PONDERADO MAYOR A 0.5
    // - REALIZADAS EN AMAZON


    //Calculamos las reglas comunes en un dataframe
    var dfTablon1 = dfTablon.filter(
      (dfTablon.col("MONTO_TRANSACCION") > 500) &&
      (dfTablon.col("RIESGO_PONDERADO") > 0.5) &&
      (dfTablon.col("NOMBRE_EMPRESA") === "Amazon")
    )

    //Mostramos los datos
    dfTablon1.show()


    //REPORTE 1:
    // - POR PERSONAS ENTRE 30 A 39 AÑOS
    // - CON UN SALARIO DE 1000 A 5000 DOLARES
    var dfReporte1 = dfTablon1.filter(
      (dfTablon1.col("EDAD_PERSONA") >= 30) &&
      (dfTablon1.col("EDAD_PERSONA") <= 39) &&
      (dfTablon1.col("SALARIO_PERSONA") >= 1000) &&
      (dfTablon1.col("SALARIO_PERSONA") <= 5000)
    )

    //Mostramos los datos
    dfReporte1.show()


    //REPORTE 2:
    // - POR PERSONAS ENTRE 40 A 49 AÑOS
    // - CON UN SALARIO DE 2500 A 7000 DOLARES
    var dfReporte2 = dfTablon1.filter(
      (dfTablon1.col("EDAD_PERSONA") >= 40) &&
      (dfTablon1.col("EDAD_PERSONA") <= 49) &&
      (dfTablon1.col("SALARIO_PERSONA") >= 2500) &&
      (dfTablon1.col("SALARIO_PERSONA") <= 7000)
    )

    //Mostramos los datos
    dfReporte2.show()


    //REPORTE 3:
    // - POR PERSONAS ENTRE 50 A 60 AÑOS
    // - CON UN SALARIO DE 3500 A 10000 DOLARES
    var dfReporte3 = dfTablon1.filter(
      (dfTablon1.col("EDAD_PERSONA") >= 50) &&
      (dfTablon1.col("EDAD_PERSONA") <= 60) &&
      (dfTablon1.col("SALARIO_PERSONA") >= 3500) &&
      (dfTablon1.col("SALARIO_PERSONA") <= 10000)
    )

    //Mostramos los datos
    dfReporte3.show()


    //////////////////////////////// Almacenamiento ////////////////////////////////

    //Almacenamos el REPORTE 1
    dfReporte1.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", ",").save("hdfs:///user/proyectos/proyecto_3/output/REPORTE_1")


    //Almacenamos el REPORTE 2
    dfReporte2.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", ",").save("hdfs:///user/proyectos/proyecto_3/output/REPORTE_2")


    //Almacenamos el REPORTE 3
    dfReporte3.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", ",").save("hdfs:///user/proyectos/proyecto_3/output/REPORTE_3")


    //Verificacion
    //Verificamos los archivos
    var dfReporteLeido1 = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("hdfs:///user/proyectos/proyecto_3/output/REPORTE_1")
    dfReporteLeido1.show()


    //Verificamos los archivos
    var dfReporteLeido2 = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("hdfs:///user/proyectos/proyecto_3/output/REPORTE_2")
    dfReporteLeido2.show()


    //Verificamos los archivos
    var dfReporteLeido3 = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("hdfs:///user/proyectos/proyecto_3/output/REPORTE_3")
    dfReporteLeido3.show()

    spark.stop()

  }
}