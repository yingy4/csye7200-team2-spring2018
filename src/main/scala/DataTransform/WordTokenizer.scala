package DataTransform


import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._



object WordTokenizer {

  def tokenize(df:Dataset[Row]): Unit = {

    val tokenizer = new Tokenizer().setInputCol("lyrics").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer().setInputCol("lyrics").setOutputCol("words").setPattern("\\W")

    val countTokens = udf { (words: Seq[String]) => words.length }

    val tokenized = tokenizer.transform(df)
    tokenized.select("lyrics", "words")
      .withColumn("tokens", countTokens(col("words"))).show(false)

    val regexTokenized = regexTokenizer.transform(df)
    regexTokenized.select("lyrics", "words")
      .withColumn("tokens", countTokens(col("words"))).show(false)

    println(tokenized)
    println(regexTokenized)

  }
}
