package MachineLearning

//import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

object Word2Vectorizer {
  val lyricsModelDirectoryPath = "/tmp/spark-logistic-regression-model"
  val genresDir = lyricsModelDirectoryPath+"/word2vec/" + "/word2vec/genres/"
  val artistsDir = lyricsModelDirectoryPath+"/word2vec/" + "/word2vec/artists/"
  private val artistsVocab = new mutable.HashMap[String, Array[Float]]()
  private val genreVocab = new mutable.HashMap[String, Array[Float]]()


  /** Group by genres and create vectors for the tokens.
    *
    *
    * @param df DataFrame
    * @return Word2Vec model that is trained with the dataframe provided
    */
  def vectorizeGenres(df:DataFrame): Word2VecModel = {
    val rdd = df.rdd.map{
      case Row(genre:String, agg:mutable.WrappedArray[String]) => agg.mkString(" ").concat(" ").concat(genre).trim.split(" ").toSeq
    }
    val word2VecModel = new Word2Vec().setMinCount(1).fit(rdd)
    word2VecModel
  }


  /** Group by artists and create vectors for the tokens.
    *
    *
    * @param df DataFrame
    * @return Word2Vec model that is trained with the dataframe provided
    */
  def vectorizeArtists(df:DataFrame): Word2VecModel = {
    val word2VecModel = new Word2Vec().setMinCount(2).fit(df.withColumn("clean_lyrics", df("clean_lyrics")).rdd.map(row => row.getString(0).split("\\s+").toSeq))
    word2VecModel
  }


  /** Find the cosine similarity between words by their vectors.
    *
    *
    * @param vec1 Vectors of first word.
    * @param vec2 Vectors of second word.
    * @return Similarity in cosine vectorization.
    */
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
}
