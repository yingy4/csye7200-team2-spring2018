package DataTransform

import FeatureExtraction.ArtistsFrequency.flattenTokensAndCount
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{collect_list, regexp_replace, udf}

import scala.collection.immutable.ListMap

object SongTokenizer {

  val flattenTokensAndCount = udf((x: String) => {
    val map = x.split("\\W+").map(x => (x, 1)).groupBy(y => y._1).map{ case (x,y) => x -> y.length }.toArray.sortBy(x => -x._2)

    ListMap(map.toSeq.sortWith(_._2 > _._2): _*) take(5)
  })

  def tokenizeSongs(df:DataFrame): DataFrame = {
    val dataframe  = df.withColumn("song_top_words", flattenTokensAndCount(df("clean_lyrics")))
    return dataframe
  }
}