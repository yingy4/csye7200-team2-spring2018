package Pipeline

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/*
class SWRemoverTransformer extends Transformer {
  final val inputCol= "tokenized_words"
  final val outputCol = "filtered_lyrics"

  def removeStopWords(df:DataFrame): DataFrame = {

    val english = StopWordsRemover.loadDefaultStopWords("english")
    val german = StopWordsRemover.loadDefaultStopWords("german")
    val french = StopWordsRemover.loadDefaultStopWords("french")

    val stop_words = english ++ german ++ french

    val remover = new StopWordsRemover()
      .setInputCol("tokenized_words")
      .setOutputCol("filtered_lyrics")
        .setStopWords(stop_words)
    val output = remover.transform(df)
    output
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = dataset.toDF()

    val english = StopWordsRemover.loadDefaultStopWords("english")
    val german = StopWordsRemover.loadDefaultStopWords("german")
    val french = StopWordsRemover.loadDefaultStopWords("french")

    val stop_words = english ++ german ++ french

    val remover = new StopWordsRemover()
      .setInputCol("tokenized_words")
      .setOutputCol("filtered_lyrics")
      .setStopWords(stop_words)
    val output = remover.transform(df)
    output

  }

  override def copy(extra: ParamMap): Transformer = super.defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = return schema

  override val uid: String = "sw"
}
*/
