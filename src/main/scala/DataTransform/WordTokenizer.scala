package DataTransform


import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._



object WordTokenizer {

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
}
