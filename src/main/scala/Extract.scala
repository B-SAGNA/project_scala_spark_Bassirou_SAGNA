import org.apache.spark.sql.{DataFrame, SparkSession}

object Extract {

  def read_source_file(path: String, format : String): DataFrame = {

    try {
      val df = Utils._spark.read.format(format).
        option("header", "true").
        option("inferSchema", "true").
        load(path)
      df
    } catch {
      case e: Exception => {
        Utils._log.error(s"The input path define in the SMC.conf is incorrect or " +
          s"the file inserted in HDFS is outdated : " +
          s"or file format is incorrect. " + e.getMessage)
        throw e
      }
    }
  }

}
