import org.apache.spark.sql.{DataFrame, SaveMode}

object Load {

  def saveData(df: DataFrame, saveMode: String, format: String, path: String): Unit = {
    try {
      df.write
        .format(format)            // Spécifie le format de sauvegarde (e.g., parquet, csv)
        .mode(saveMode)            // Spécifie le mode de sauvegarde (e.g., overwrite, append)
        .save(path)                // Spécifie le chemin de sauvegarde
    } catch {
      case e: Exception => {
        Utils._log.error(s"Erreur lors de la sauvegarde des données: ${e.getMessage}")
        throw e
      }
    }
  }

}
