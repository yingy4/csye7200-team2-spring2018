package FeatureExtraction

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{collect_list, udf}

object GenreFrequency {
  val flatten = udf((xs: Seq[Seq[String]]) => xs.flatten.foldLeft(Map[String,Int]() withDefaultValue 0){
    (word,occurence) => word + (occurence -> (1 + word(occurence)))
  })

  def filterByGenreFrequency(df:DataFrame): DataFrame = {
    val genreCommonWords = df.groupBy("genre").agg(flatten(collect_list(df("words")))).as("set").sort()
    return genreCommonWords
  }
}
