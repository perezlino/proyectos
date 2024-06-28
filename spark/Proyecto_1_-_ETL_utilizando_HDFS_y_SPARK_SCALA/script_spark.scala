// Librerías

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

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-scala-etl")
      .master("spark://spark-master:7077")
      .config("spark.version", "2.4.5")
      .getOrCreate()

    //Lectura de datos
    //Leemos el archivo indicando el esquema
    var dfPersona = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
      StructType(
        Array(
          StructField("ID", StringType, true),
          StructField("NOMBRE", StringType, true),
          StructField("TELEFONO", StringType, true),
          StructField("CORREO", StringType, true),
          StructField("FECHA_INGRESO", StringType, true),
          StructField("EDAD", IntegerType, true),
          StructField("SALARIO", DoubleType, true),
          StructField("ID_EMPRESA", StringType, true)
        )
      )
    ).load("hdfs:///user/proyectos/proyecto_2/input/DATA_PERSONA.txt")

    //Vemos el esquema
    dfPersona.printSchema()

    //Mostramos los datos
    dfPersona.show()


    //Leemos el archivo indicando el esquema
    var dfEmpresa = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
      StructType(
        Array(
          StructField("ID", StringType, true),
          StructField("NOMBRE", StringType, true)
        )
      )
    ).load("hdfs:///user/proyectos/proyecto_2/input/DATA_EMPRESA.txt")

    //Vemos el esquema
    dfEmpresa.printSchema()

    //Mostramos los datos
    dfEmpresa.show()


    //Leemos el archivo indicando el esquema
    var dfTransaccion = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
      StructType(
        Array(
          StructField("ID_PERSONA", StringType, true),
          StructField("ID_EMPRESA", StringType, true),
          StructField("MONTO", DoubleType, true),
          StructField("FECHA", StringType, true)
        )
      )
    ).load("hdfs:///user/proyectos/proyecto_2/input/DATA_TRANSACCION.txt")

    //Vemos el esquema
    dfTransaccion.printSchema()

    //Mostramos los datos
    dfTransaccion.show()


    //Reglas de calidad
    //Aplicamos las reglas de calidad a dfTransaccion
    var dfTransaccionLimpio = dfTransaccion.filter(
      (dfTransaccion.col("ID_PERSONA").isNotNull) &&
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
      (dfPersona.col("ID").isNotNull) &&
        (dfPersona.col("NOMBRE").isNotNull) &&
        (dfPersona.col("SALARIO").isNotNull) &&
        (dfPersona.col("SALARIO") > 0)
    )

    //Vemos el esquema
    dfPersonaLimpio.printSchema()

    //Mostramos los datos
    dfPersonaLimpio.show()


    //Aplicamos las reglas de calidad a dfEmpresa
    var dfEmpresaLimpio = dfEmpresa.filter(
      (dfEmpresa.col("ID").isNotNull) &&
        (dfEmpresa.col("NOMBRE").isNotNull)
    )

    //Vemos el esquema
    dfEmpresaLimpio.printSchema()

    //Mostramos los datos
    dfEmpresaLimpio.show()


    //Preparación de tablones
    //PASO 1 (<<T_P>>): AGREGAR LOS DATOS DE LAS PERSONAS QUE REALIZARON LAS TRANSACCIONES
    var df1 = dfTransaccionLimpio.join(
      dfPersonaLimpio,
      dfTransaccionLimpio.col("ID_PERSONA") === dfPersonaLimpio.col("ID"),
      "inner"
    ).select(
      dfTransaccionLimpio.col("ID_PERSONA"),
      dfPersonaLimpio.col("NOMBRE").alias("NOMBRE_PERSONA"),
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
      df1.col("ID_EMPRESA") === dfEmpresaLimpio.col("ID"),
      "inner"
    ).select(
      df1.col("ID_PERSONA"),
      df1.col("NOMBRE_PERSONA"),
      df1.col("EDAD_PERSONA"),
      df1.col("SALARIO_PERSONA"),
      df1.col("ID_EMPRESA"),
      dfEmpresaLimpio.col("NOMBRE").alias("NOMBRE_EMPRESA"),
      df1.col("MONTO_TRANSACCION"),
      df1.col("FECHA_TRANSACCION")
    )

    //Mostramos los datos
    df2.show()


    //Procesamiento
    //REPORTE 1:
    //
    // - TRANSACCIONES MAYORES A 500 DÓLARES
    // - REALIZADAS EN AMAZON
    // - POR PERSONAS ENTRE 30 A 39 AÑOS
    // - CON UN SALARIO DE 1000 A 5000 DOLARES
    //
    //REPORTE 2:
    //
    // - TRANSACCIONES MAYORES A 500 DÓLARES
    // - REALIZADAS EN AMAZON
    // - POR PERSONAS ENTRE 40 A 49 AÑOS
    // - CON UN SALARIO DE 2500 A 7000 DOLARES
    //
    //REPORTE 3:
    //
    // - TRANSACCIONES MAYORES A 500 DÓLARES
    // - REALIZADAS EN AMAZON
    // - POR PERSONAS ENTRE 50 A 60 AÑOS
    // - CON UN SALARIO DE 3500 A 10000 DOLARES
    //
    //Notamos que todos los reportes comparten:
    // - TRANSACCIONES MAYORES A 500 DÓLARES
    // - REALIZADAS EN AMAZON


    //Calculamos las reglas comunes en un dataframe
    var dfTablon = df2.filter(
      (df2.col("MONTO_TRANSACCION") > 500) &&
        (df2.col("NOMBRE_EMPRESA") === "Amazon")
    )

    //Mostramos los datos
    dfTablon.show()


    //REPORTE 1:
    // - POR PERSONAS ENTRE 30 A 39 AÑOS
    // - CON UN SALARIO DE 1000 A 5000 DOLARES
    var dfReporte1 = dfTablon.filter(
      (dfTablon.col("EDAD_PERSONA") >= 30) &&
        (dfTablon.col("EDAD_PERSONA") <= 39) &&
        (dfTablon.col("SALARIO_PERSONA") >= 1000) &&
        (dfTablon.col("SALARIO_PERSONA") <= 5000)
    )

    //Mostramos los datos
    dfReporte1.show()


    //REPORTE 2:
    // - POR PERSONAS ENTRE 40 A 49 AÑOS
    // - CON UN SALARIO DE 2500 A 7000 DOLARES
    var dfReporte2 = dfTablon.filter(
      (dfTablon.col("EDAD_PERSONA") >= 40) &&
        (dfTablon.col("EDAD_PERSONA") <= 49) &&
        (dfTablon.col("SALARIO_PERSONA") >= 2500) &&
        (dfTablon.col("SALARIO_PERSONA") <= 7000)
    )

    //Mostramos los datos
    dfReporte2.show()


    //REPORTE 3:
    // - POR PERSONAS ENTRE 50 A 60 AÑOS
    // - CON UN SALARIO DE 3500 A 10000 DOLARES
    var dfReporte3 = dfTablon.filter(
      (dfTablon.col("EDAD_PERSONA") >= 50) &&
        (dfTablon.col("EDAD_PERSONA") <= 60) &&
        (dfTablon.col("SALARIO_PERSONA") >= 3500) &&
        (dfTablon.col("SALARIO_PERSONA") <= 10000)
    )

    //Mostramos los datos
    dfReporte3.show()


    //Almacenamiento
    //Almacenamos el REPORTE 1
    dfReporte1.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", ",").save("hdfs:///user/proyectos/proyecto_2/output/REPORTE_1")


    //Almacenamos el REPORTE 2
    dfReporte2.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", ",").save("hdfs:///user/proyectos/proyecto_2/output/REPORTE_2")


    //Almacenamos el REPORTE 3
    dfReporte3.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", ",").save("hdfs:///user/proyectos/proyecto_2/output/REPORTE_3")


    //Verificacion
    //Verificamos los archivos
    var dfReporteLeido1 = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("hdfs:///user/proyectos/proyecto_2/output/REPORTE_1")
    dfReporteLeido1.show()


    //Verificamos los archivos
    var dfReporteLeido2 = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("hdfs:///user/proyectos/proyecto_2/output/REPORTE_2")
    dfReporteLeido2.show()


    //Verificamos los archivos
    var dfReporteLeido3 = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("hdfs:///user/proyectos/proyecto_2/output/REPORTE_3")
    dfReporteLeido3.show()

    spark.stop()

  }
}