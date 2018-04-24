package DataTransform

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{DataFrame, Row}

object SWRemover {

  /** Remove the english, french and german stop words from and return the dataframe with the filtered lyrics column created. This column has the lyrics with stop words removed.
    *
    *
    * @param df Datframe from which the stop words have to be removed.
    * @return
    */
  def removeStopWords(df:DataFrame): DataFrame = {

    val english = StopWordsRemover.loadDefaultStopWords("english")
    val german = StopWordsRemover.loadDefaultStopWords("german")
    val french = StopWordsRemover.loadDefaultStopWords("french")

    val stop_words = english ++ german ++ french

    val remover = new StopWordsRemover()
      .setInputCol("clean_tokens")
      .setOutputCol("filtered lyrics")
      .setStopWords(stop_words)
    val output = remover.transform(df)
    output
  }
}