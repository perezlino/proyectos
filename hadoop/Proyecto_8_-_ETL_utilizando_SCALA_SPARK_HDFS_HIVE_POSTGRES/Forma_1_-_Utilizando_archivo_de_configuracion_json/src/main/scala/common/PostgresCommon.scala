package common

import java.util.Properties
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object PostgresCommon {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def getPostgresCommonProps() : Properties = {
    logger.info("El metodo getPostgresCommonProps() ha iniciado")
    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user","postgres")
    pgConnectionProperties.put("password","admin")
    logger.info("El metodo getPostgresCommonProps() ha finalizado")
    pgConnectionProperties
  }

  def getPostgresServerDatabase() : String = {
    logger.info("El metodo getPostgresServerDatabase() ha iniciado")
    val pgURL = "jdbc:postgresql://postgres:5432/sparkdb"
    logger.info("El metodo getPostgresServerDatabase() ha finalizado")
    pgURL
  }

  def fetchDataFrameFromPgTable(spark:SparkSession, pgTable:String):Option[DataFrame] ={
    try {
      logger.info("El metodo fetchDataFrameFromPgTable() ha iniciado")
      val pgProp = getPostgresCommonProps()
      val pgURLdetails = getPostgresServerDatabase()
      val pgCourseDataframe = spark.read.jdbc(pgURLdetails,pgTable,pgProp)
      logger.info("El metodo fetchDataFrameFromPgTable() ha finalizado")
      Some(pgCourseDataframe)
    }
      catch {
      case e:Exception =>
        logger.error("Ha ocurrido un error en el metodo fetchDataFrameFromPgTable() "+ e.printStackTrace())
        // Salir con código de salida 1 en caso de error
        System.exit(1)
        None
    }
  }
  def writeDFToPostgresTable(dataFrame:DataFrame, pgTable:String):Unit = {
    try {
      logger.info("El metodo writeDFToPostgresTable() ha iniciado")

      dataFrame.write
        .mode(SaveMode.Append)
        .format("jdbc")
        .option("url",getPostgresServerDatabase())
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", pgTable)
        .option("user", "airflow")
        .option("password", "airflow")
        .save()
      logger.info("El metodo writeDFToPostgresTable() ha finalizado")

    } catch {
      case e: Exception =>
        logger.error("Ha ocurrido un error en el metodo writeDFToPostgresTable() "+ e.printStackTrace())
    }
  }
}

