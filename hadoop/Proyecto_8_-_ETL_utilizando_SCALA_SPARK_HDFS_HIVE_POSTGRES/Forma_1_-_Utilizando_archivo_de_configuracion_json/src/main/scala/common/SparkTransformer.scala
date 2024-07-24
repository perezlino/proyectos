package common

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object SparkTransformer {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def reemplazarValoresNull(dataFrame : DataFrame) : DataFrame = {
    logger.info("El metodo reemplazarValoresNull() ha iniciado")
    val valor_nombre_autor : String = CursosJsonParser.returnConfigValue("body." +
      "reemplazar_valores_null.nombre_autor")
    logger.info("valor_nombre_autor "+ valor_nombre_autor)
    val valor_num_opiniones : String = CursosJsonParser.returnConfigValue("body." +
      "reemplazar_valores_null.num_opiniones")
    logger.info("num_opiniones "+ valor_num_opiniones)

    if ((valor_nombre_autor == "SI") &&
      (valor_num_opiniones == "SI")) {
      logger.warn("Ambos SI ")
      val transformedDF = dataFrame.na.fill("Unknown",Seq("nombre_autor"))
        .na.fill(value = "0",Seq("num_opiniones"))
      logger.info("El metodo reemplazarValoresNull() ha finalizado")
      transformedDF
    } else if ((valor_nombre_autor == "SI") &&
      (valor_num_opiniones == "NO")) {
      logger.warn("Only Author Name SI ")
      val transformedDF = dataFrame.na.fill("Unknown",Seq("nombre_autor"))
      logger.info("El metodo reemplazarValoresNull() ha finalizado")
      transformedDF
    } else if ((valor_nombre_autor == "NO") &&
      (valor_num_opiniones == "SI")) {
      logger.warn("Only Number of Review SI ")
      val transformedDF = dataFrame.na.fill(value = "0",Seq("num_opiniones"))
      logger.info("El metodo reemplazarValoresNull() ha finalizado")
      transformedDF
    } else {
      dataFrame
    }
  }
}