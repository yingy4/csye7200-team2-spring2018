package DataTransform


import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._



object WordTokenizer {
  def tokenize(df:DataFrame, inColumnName : String, outColumnName:String): DataFrame = {
    val regexTokenizer = new RegexTokenizer()
      .setInputCol(inColumnName)
      .setOutputCol(outColumnName)
      .setPattern("\\W")
    val countTokens = udf { (words: Seq[String]) => words.length }

    val out = regexTokenizer.transform(df)
      .withColumn("tokens", countTokens(col(outColumnName)))
    out
  }
}