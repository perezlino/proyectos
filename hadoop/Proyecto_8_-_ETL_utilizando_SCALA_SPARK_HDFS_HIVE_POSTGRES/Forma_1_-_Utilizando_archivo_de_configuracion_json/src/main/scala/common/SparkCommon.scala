package common

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.slf4j.LoggerFactory

object SparkCommon {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def crearSparkSession():Option[SparkSession] = {
    try {
      // Crear la Spark Session
      logger.info("Iniciando el metodo createSparkSession()")
      // System.setProperty("hadoop.home.dir", "C:\\PySpark_Installed\\Hadoop\\winutils")
      // .config("spark.sql.warehouse.dir",warehouseLocation)
      val spark = SparkSession
        .builder
        .appName("spark-scala-etl")
        .master("spark://spark-master:7077")
        .config("spark.version", "2.4.5")
        .enableHiveSupport()
        .getOrCreate()

      // Devolver la Spark Session
      logger.info("Devolviendo la Spark Session")
      Some(spark)  // Devolver Some con la instancia de SparkSession

    } catch {
      case e: Exception =>
        logger.error("Error al crear SparkSession" + e.printStackTrace())
        // Salir con código de salida 1 en caso de error
        System.exit(1)
        None  // Devolver None en caso de error
    }
  }

  def crearTablaHiveCursos(spark:SparkSession):Unit = {
    logger.info("El metodo crearTablaHiveCursos() se ha iniciado")
    spark.sql(""" create database if not exists sparkdb location '/user/hive/warehouse/sparkdb' """)
    spark.sql(""" use sparkdb """)
    spark.sql(""" create table if not exists sparkdb.tabla_cursos(id_curso string,nombre_curso string, nombre_autor string,num_opiniones string) """)
    spark.sql(""" insert into sparkdb.tabla_cursos VALUES (1,'Java','FutureX',45) """)
    spark.sql(""" insert into sparkdb.tabla_cursos VALUES (2,'Java','FutureXSkill',56) """)
    spark.sql(""" insert into sparkdb.tabla_cursos VALUES (3,'Big Data','Future',100) """)
    spark.sql(""" insert into sparkdb.tabla_cursos VALUES (4,'Linux','Future',100) """)
    spark.sql(""" insert into sparkdb.tabla_cursos VALUES (5,'Microservices','Future',100) """)
    spark.sql(""" insert into sparkdb.tabla_cursos VALUES (6,'CMS','',100) """)
    spark.sql(""" insert into sparkdb.tabla_cursos VALUES (7,'Python','FutureX','') """)
    spark.sql(""" insert into sparkdb.tabla_cursos VALUES (8,'CMS','Future',56) """)
    spark.sql(""" insert into sparkdb.tabla_cursos VALUES (9,'Dot Net','FutureXSkill',34) """)
    spark.sql(""" insert into sparkdb.tabla_cursos VALUES (10,'Ansible','FutureX',123) """)
    spark.sql(""" insert into sparkdb.tabla_cursos VALUES (11,'Jenkins','Future',32) """)
    spark.sql(""" insert into sparkdb.tabla_cursos VALUES (12,'Chef','FutureX',121) """)
    spark.sql(""" insert into sparkdb.tabla_cursos VALUES (13,'Go Lang','',105) """)

    // Tratar strings vacíos como Null
    spark.sql(""" alter table sparkdb.tabla_cursos set tblproperties('serialization.null.format'='') """)

  }

  def leerTablaHiveCursos(spark:SparkSession):Option[DataFrame] = {
    try {
      logger.info("Se ha iniciado el metodo leerTablaHiveCursos()")
      val cursoDF = spark.sql("select * from sparkdb.tabla_cursos")
      logger.info("Ha finalizado el metodo leerTablaHiveCursos()")
      Some(cursoDF)
    } catch {
      case e: Exception =>
        logger.error("Error en la lectura de la tabla sparkdb.tabla_cursos " + e.printStackTrace())
        None
    }
  }

  def escribirTablaHive(spark: SparkSession, df: DataFrame, hiveTable: String): Unit = {
    try {
      logger.info("El metodo escribirTablaHive() se ha iniciado")

      // Crear vista temporal
      val tmpView = hiveTable + "_tempView"
      df.createOrReplaceTempView(tmpView)

      // Utilizar la base de datos sparkdb
      spark.sql("USE sparkdb")

      // Verificar si la tabla Hive existe
      val tableExists = spark.sql(s"SHOW TABLES LIKE '$hiveTable'").count() > 0

      if (!tableExists) {
        // Si la tabla no existe, crearla
        spark.sql(s"CREATE TABLE $hiveTable AS SELECT * FROM $tmpView")
      } else {
        // Si la tabla ya existe, sobrescribirla
        spark.sql(s"INSERT OVERWRITE TABLE $hiveTable SELECT * FROM $tmpView")
      }

      logger.info("El metodo escribirTablaHive() se ha finalizado")
    } catch {
      case e: Exception =>
        logger.error("Error en la escritura de la tabla Hive", e)
    }
  }
}