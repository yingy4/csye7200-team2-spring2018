package Pipeline

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}


class WordTokenizerTransformer extends Transformer {
  final val inputCol= "lyrics"
  final val outputCol = "tokens"

  def tokenize(df:DataFrame, columnName:String): DataFrame = {
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("lyrics")
      .setOutputCol(columnName)
      .setPattern("\\W")
    val countTokens = udf { (words: Seq[String]) => words.length }

    val out = regexTokenizer.transform(df)
      .withColumn("tokens", countTokens(col(columnName)))
    out
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = dataset.toDF()
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("lyrics")
      .setOutputCol("tokens")
      .setPattern("\\W")
    val countTokens = udf { (words: Seq[String]) => words.length }
    val out = regexTokenizer.transform(df)
      .withColumn("count_tokens", countTokens(col("tokens")))
    out

  }

  /*
  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setCountOutputCol(value: String): this.type = set(countOutCol, value)


  def getInputCol(value: String): this.type = set(inputCol, value)

  def getOutputCol(value: String): this.type = set(outputCol, value)

  def getCountOutputCol(value: String): this.type = set(countOutCol, value)
*/

  override def copy(extra: ParamMap): Transformer = super.defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = return schema

  override val uid: String = "wt"
}
