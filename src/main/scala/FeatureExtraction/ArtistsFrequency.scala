package FeatureExtraction

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

object ArtistsFrequency {
  val flatten = udf((xs: Seq[Seq[String]]) => xs.flatten.foldLeft(Map[String,Int]() withDefaultValue 0){
    (word,occurence) => word + (occurence -> (1 + word(occurence)))
    })

  def filterByArtistFrequency(df:DataFrame): DataFrame = {
    val artistCommonWords = df.groupBy("artist").agg(flatten(collect_list(df("words")))).as("set").sort()
    println("---------------------------------start test-------------------------------------------------")
    artistCommonWords.show(20, false)
    println("---------------------------------test-------------------------------------------------")
    return artistCommonWords
  }

}
