package FeatureExtraction

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.spark_project.dmg.pmml.False

import scala.collection.immutable.ListMap


object ArtistsFrequency {
  val flattenTokensAndCount = udf((xs: Seq[Seq[String]]) => {
    val map = xs.flatten.foldLeft(Map[String, Int]() withDefaultValue 0) {
      (word, occurence) => word + (occurence -> (1 + word(occurence)))
    }

    ListMap(map.toSeq.sortWith(_._2 > _._2): _*) take(5)
  })

  val flattenTokens = udf((xs: Seq[Seq[String]]) => xs.flatten.distinct)

/*
  def filterByArtistFrequency(df:DataFrame): DataFrame = {
    val artistCommonWords = df.groupBy("artist").agg(flattenTokensAndCount(collect_list(df("words"))).as("artists_tokens"))
    //artistCommonWords.show(false)
    return artistCommonWords
  }

  def groupLyrics(df:DataFrame): DataFrame = {
    val artistCommonLyrics = df.groupBy("artist").agg(flattenTokens(collect_list(df("words"))).as("tokenized_words"))
    //artistCommonLyrics.show(false)
    return artistCommonLyrics
  }*/
}
