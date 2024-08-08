import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Transform {

  /**
   * Cette fonction permet de nettoyer les données brutes.
   * @param df : Dataframe
   * @return Dataframe
   */
  def cleanData(df: DataFrame): DataFrame = {
    /*
     0. Traiter toutes les colonnes en date timestamp vers YYYY/MM/DD HH:MM SSS.
     On suppose que les colonnes de type timestamp sont `event_previous_timestamp`, `event_timestamp`, `user_first_touch_timestamp`.
     */

    // Convertir les colonnes timestamp
    val firstDF = df.withColumn("event_previous_timestamp", to_timestamp(col("event_previous_timestamp")))
      .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
      .withColumn("user_first_touch_timestamp", to_timestamp(col("user_first_touch_timestamp")))

    /*
     1. Extraire les revenus d'achat pour chaque événement
      - Ajouter une nouvelle colonne nommée revenue en faisant l'extraction de ecommerce.purchase_revenue_in_usd
     */

    val revenueDF = firstDF.withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))

    /*
     2. Filtrer les événements dont le revenu n'est pas null
     */
    val purchasesDF = revenueDF.filter(col("revenue").isNotNull)

    /*
     3. Quels sont les types d'événements qui génèrent des revenus ?
      Trouvez des valeurs event_name uniques dans purchasesDF.
      Combien y a t-il de type d'evenement ?
     */

    val distinctDF = purchasesDF.select("event_name").distinct()

    /*
     4. Supprimer la/les colonne(s) inutile(s)
      - Supprimer event_name de purchasesDF.
     */

    val cleanDF = purchasesDF.drop("event_name")

    cleanDF
  }

  /**
   * Cette fonction permet de récupérer le revenu cumulé par source de trafic.
   * @param df : Dataframe
   * @return Dataframe
   */
  def computeTrafficRevenue(df: DataFrame): DataFrame = {
    /*
     5. Revenus cumulés par source de trafic (modifiez en fonction des colonnes disponibles)
      - Obtenez la somme de revenue comme total_rev
      - Obtenez la moyenne de revenue comme avg_rev
     */

    // Modifiez les colonnes en fonction des colonnes disponibles
    val trafficDF = df.groupBy("traffic_source")
      .agg(
        sum("revenue").alias("total_rev"),
        avg("revenue").alias("avg_rev")
      )

    /*
     6. Récupérer les cinq principales sources de trafic par revenu total
     */
    val topTrafficDF = trafficDF.orderBy(desc("total_rev")).limit(5)

    /*
     7. Limiter les colonnes de revenus à deux décimales pointées
      Modifier les colonnes avg_rev et total_rev pour les convertir en des nombres avec deux décimales pointées
     */
    val finalDF = topTrafficDF.withColumn("total_rev", format_number(col("total_rev"), 2))
      .withColumn("avg_rev", format_number(col("avg_rev"), 2))

    finalDF
  }
}
