package FeatureExtraction

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{collect_list, udf}

object GenreFrequency {
  val flattenTokensAndCount = udf((xs: Seq[Seq[String]]) => xs.flatten.foldLeft(Map[String,Int]() withDefaultValue 0){
    (word,occurence) => word + (occurence -> (1 + word(occurence)))
  })

  val flattenTokens = udf((xs: Seq[Seq[String]]) => xs.flatten.distinct)

  def filterByGenreFrequency(df:DataFrame): DataFrame = {
    val genreCommonWords = df.groupBy("genre").agg(flattenTokensAndCount(collect_list(df("words"))).as("genre_tokens"))
    //genreCommonWords.collect().foreach(println)
    return genreCommonWords
  }

  def groupLyrics(df:DataFrame): DataFrame = {
    val genreCommonLyrics = df.groupBy("genre").agg(flattenTokens(collect_list(df("words"))).as("tokenized_words"))
    //artistCommonLyrics.show(false)
    return genreCommonLyrics
  }
}
