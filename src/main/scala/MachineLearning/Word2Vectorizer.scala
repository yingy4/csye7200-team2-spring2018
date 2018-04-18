package MachineLearning

//import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object Word2Vectorizer {
  val lyricsModelDirectoryPath = "/tmp/spark-logistic-regression-model"
  val genresDir = lyricsModelDirectoryPath+"/word2vec/" + "/word2vec/genres/"
  val artistsDir = lyricsModelDirectoryPath+"/word2vec/" + "/word2vec/artists/"
  private val artistsVocab = new mutable.HashMap[String, Array[Float]]()
  private val genreVocab = new mutable.HashMap[String, Array[Float]]()


  /*
  def vectorize(df:DataFrame): Word2VecModel = {
    val word2Vec = new Word2Vec().setInputCol("words").setOutputCol("features").setVectorSize(4).setMinCount(1)

    val out_df = word2Vec.fit(df)
    out_df
  }
  */

  def vectorizeGenres(df:DataFrame): Word2VecModel = {
    val word2VecModel = new Word2Vec().setMinCount(2).fit(df.withColumn("lyricsWithGenre", df("lyricsWithGenre")).rdd.map(row => Seq(row.getString(0))))
    word2VecModel
  }

  def vectorizeArtists(df:DataFrame): Word2VecModel = {
    val word2VecModel = new Word2Vec().setMinCount(2).fit(df.withColumn("lyricsWithGenre", df("lyricsWithGenre")).rdd.map(row => Seq(row.getString(0))))
    word2VecModel
  }

  def saveGenres(spark:SparkContext, word2VecModel: Word2VecModel): Unit = {
    word2VecModel.save(spark, genresDir)
  }

  def saveArtists(spark: SparkContext, word2VecModel: Word2VecModel): Unit = {
    word2VecModel.save(spark, artistsDir)
  }


  def loadGenres(spark: SparkContext): Word2VecModel = {
    Word2VecModel.load(spark, genresDir)
  }

  def loadArtists(spark: SparkContext): Word2VecModel = {
    Word2VecModel.load(spark, artistsDir)
  }



  def cosine(vec1: Array[Double], vec2: Array[Double]): Double = {
    assert(vec1.length == vec2.length, "Uneven vectors!")
    var dot, sum1, sum2 = 0.0
    for (i <- 0 until vec1.length) {
      dot += (vec1(i) * vec2(i))
      sum1 += (vec1(i) * vec1(i))
      sum2 += (vec2(i) * vec2(i))
    }
    dot / (math.sqrt(sum1) * math.sqrt(sum2))
  }


  /*def cosineArtists(word1: String, word2: String): Double = {
    assert(containsArtists(word1) && containsArtists(word2), "Out of dictionary word! " + word1 + " or " + word2)
   // cosine(artistsVocab.get(word1).get, artistsVocab.get(word2).get)
  }*/

  def containsArtists(word: String): Boolean = {
    artistsVocab.get(word).isDefined
  }
}
