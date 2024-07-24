package Main

import common.{CursosJsonParser, PostgresCommon, SparkCommon, SparkTransformer}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkScala {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      logger.info("El metodo main() ha iniciado")
      val spark: SparkSession = SparkCommon.crearSparkSession().get

      // Crear tabla "sparkdb.tabla_cursos" en Hive
      logger.info("La creación de la tabla en Hive ha iniciado")
      SparkCommon.crearTablaHiveCursos(spark)

      // Devolver la tabla "sparkdb.tabla_cursos" en un Dataframe
      logger.info("La creación del Dataframe de la tabla Hive ha iniciado")
      val cursoDF = SparkCommon.leerTablaHiveCursos(spark).get
      cursoDF.show()

      // Reemplazar valores Null
      logger.info("La transformacion de datos del Dataframe de la tabla Hive ha iniciado")
      val transformarDF1 = SparkTransformer.reemplazarValoresNull(cursoDF)
      transformarDF1.show()

      // Persistir tabla Hive en Postgres
      //val pgTablaCurso = "sparkschema.tabla_cursos"
      logger.info("La escritura del Dataframe de la tabla Hive en Postgres ha iniciado")
      val pgTablaCurso = CursosJsonParser.fetchPGTargetTable()
      PostgresCommon.writeDFToPostgresTable(transformarDF1,pgTablaCurso)

      // Exportar Dataframe en un archivo CSV
      logger.info("La exportacion del Dataframe de la tabla Hive a un archivo CSV ha iniciado")
      // La ruta '/user/hive/archivos' no es necesaria crearla, se creará en runtime
      transformarDF1.write.format("csv").save("/user/hive/archivos")

      // Persistir el Dataframe en una nueva tabla Hive
      logger.info("La exportacion del Dataframe a una nueva tabla Hive ha iniciado")
      SparkCommon.escribirTablaHive(spark,transformarDF1,"nueva_tabla_cursos")

    //
    //      //Crear un DataFrame desde la tabla "spark_table" de Postgres
    //      logger.info("Creando un Dataframe desde Postgres")
    //      val pgTable = "sparkschema.spark_table"
    //      //server:port/database_name
    //      val pgCourseDataframe = PostgresCommon.fetchDataFrameFromPgTable(spark,pgTable).get
    //      logger.info("\nSe han extraido los datos !!!")
    //      pgCourseDataframe.show()
    //      logger.info("Ha finalizado la visualización")
    } catch {
      case e:Exception =>
        logger.error("Ha ocurrido un error en el metodo main() "+ e.printStackTrace())
    }
  }
}