package DataTransform

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{DataFrame, Row}

object SWRemover {

  def removeStopWords(df:DataFrame): DataFrame = {

    val english = StopWordsRemover.loadDefaultStopWords("english")
    val german = StopWordsRemover.loadDefaultStopWords("german")
    val french = StopWordsRemover.loadDefaultStopWords("french")

    val stop_words = english ++ german ++ french


    val remover = new StopWordsRemover()
      .setInputCol("tokenized_words")
      .setOutputCol("filtered lyrics")
      .setStopWords(stop_words)
    val output = remover.transform(df)
    output
  }
}