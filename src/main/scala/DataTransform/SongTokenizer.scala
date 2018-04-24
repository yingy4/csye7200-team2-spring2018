package DataTransform

import FeatureExtraction.ArtistsFrequency.flattenTokensAndCount
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{collect_list, regexp_replace, udf}

import scala.collection.immutable.ListMap
import scala.collection.mutable



object SongTokenizer {
  val getTopWords = udf((x: String) => {
    val map: Array[(String, Int)] = x.split("\\W+").map(x => (x, 1)).groupBy(y => y._1).map{ case (x,y) => x -> y.length }.toArray.sortBy(x => -x._2)
    val m = ListMap(map.toSeq.sortWith(_._2 > _._2): _*).map(a => (a._2, a._1))
    m.values.toList
  })

  /** Tokenize the words in the cleaned lyrics, and; create and add the top 3 words for each song to th top1, top2 and top3 column.
    *
    *
    * @param spark SparkSession
    * @param df Dataframe to transform
    * @return Dataframe with the top1, top2, top3 columns added to it.
    */
  def tokenizeSongs(spark: SparkSession,df :DataFrame): DataFrame = {
    val topWordsList = getTopWords(df("clean_lyrics"))
    val dataframe  = df.withColumn("top1", topWordsList(0))
                       .withColumn("top2", topWordsList(1))
                       .withColumn("top3", topWordsList(2))
    dataframe
  }
}