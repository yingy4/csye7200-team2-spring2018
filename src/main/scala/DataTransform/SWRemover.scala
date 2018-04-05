package DataTransform

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{Dataset, Row}

object SWRemover {

  def removeStopWords(df:Dataset[Row]): Unit = {

    val remover = new StopWordsRemover()
      .setInputCol("lyrics")
      .setOutputCol("filtered lyrics")
        .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))

    remover.transform(df)

  }
}
