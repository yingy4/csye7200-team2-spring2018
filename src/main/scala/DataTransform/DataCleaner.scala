package DataTransform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.regexp_replace

object DataCleaner {

  /** Clean the dataframe by removing from the lyrics column non alpha-numeric characters, new line characters, tab characters and single quotes.
    * The cleaned lyrics is added to the clean_lyrics column that is created.
    *
    * @param df Dataframe to clean and add the clean_lyrics column with the lyrics cleaned.
    * @return dataframe with clean_lyrics column added to it.
    */
  def cleanTrain(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("lyrics").isNotNull).withColumn("clean_lyrics", regexp_replace(df("lyrics"), "[\\',\\n,\\t,\\r,\\r\\n,\\,]", ""))
    dataframe
  }
}