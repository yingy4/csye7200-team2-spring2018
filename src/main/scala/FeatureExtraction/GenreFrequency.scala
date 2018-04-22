package FeatureExtraction

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{collect_list, udf}

import scala.collection.immutable.ListMap

object GenreFrequency {
  val flattenTokensAndCount = udf((xs: Seq[Seq[String]]) => {
    val map = xs.flatten.foldLeft(Map[String, Int]() withDefaultValue 0) {
      (word, occurence) => word + (occurence -> (1 + word(occurence)))
    }
    ListMap(map.toSeq.sortWith(_._2 > _._2): _*) take(5)
  })



  val flattenTokens = udf((xs: Seq[Seq[String]]) => xs.flatten.distinct)

 /* def filterByGenreFrequency(df:DataFrame): DataFrame = {
    val genreCommonWords = df.groupBy("genre").agg(flattenTokensAndCount(collect_list(df("words"))).as("genre_tokens"))
    return genreCommonWords
  }
*/
  def groupLyrics(df:DataFrame): DataFrame = {
    val genreCommonLyrics = df.groupBy("genre").agg(flattenTokens(collect_list(df("clean_tokens"))).as("agg_clean_lyrics"))
    return genreCommonLyrics
  }
}
