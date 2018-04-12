package DataTransform

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{DataFrame, Row}

object SWRemover {

  def removeStopWords(df:DataFrame): DataFrame = {

    val remover = new StopWordsRemover()
      .setInputCol("tokenized_words")
      .setOutputCol("filtered lyrics")
        .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
    val output = remover.transform(df)
    output
  }
}
