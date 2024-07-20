import logging
import logging.config

### Eejcutar el archivo de configuracion Logging
logging.config.fileConfig(fname='../util/logging_to_file.conf')

# Obtener el custom Logger desde el archivo de configuracion Logging
logger = logging.getLogger(__name__)

def extract_files(df,format,filePath,split_no,headerReq,compressionType):
    try:
        logger.info(f"Extraccion - extract_files() se ha iniciado...")
        df.coalesce(split_no) \
          .write \
          .format(format) \
          .save(filePath, header=headerReq, compression=compressionType)
    except Exception as exp:
        logger.error("Error en el metodo - extract(). Por favor chequear el Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("Extraccion - extract_files() a finalizado...")
