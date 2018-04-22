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

    /*
    val tempDF = spark.createDataFrame(Seq(m.values.toSeq)).toDF("raw")

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val topWordsList = remover.transform(tempDF).rdd.collect()(0) match {
      case Row(arr : mutable.WrappedArray[String]) => arr.toList
    }

    topWordsList
    */
    m.values.toList
  })

  def tokenizeSongs(spark: SparkSession,df :DataFrame): DataFrame = {
    val topWordsList = getTopWords(df("clean_lyrics"))
    val dataframe  = df.withColumn("top1", topWordsList(0))
                       .withColumn("top2", topWordsList(1))
                       .withColumn("top3", topWordsList(2))
    return dataframe
  }
}