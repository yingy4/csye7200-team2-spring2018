package MachineLearning

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object Word2Vectorizer {
  val lyricsModelDirectoryPath = "/tmp/spark-logistic-regression-model"
  val genresDir = lyricsModelDirectoryPath+"/word2vec/" + "/word2vec/genres/"
  val artistsDir = lyricsModelDirectoryPath+"/word2vec/" + "/word2vec/artists/"
  private val artistsVocab = new mutable.HashMap[String, Array[Float]]()
  private val genreVocab = new mutable.HashMap[String, Array[Float]]()


  def vectorize(df:DataFrame): Word2VecModel = {
    val word2Vec = new Word2Vec().setInputCol("words").setOutputCol("features").setVectorSize(4).setMinCount(1)
    val out_df = word2Vec.fit(df)
    out_df
  }

  def vectorizeGenres(df:DataFrame): Word2VecModel = {
    val word2Vec = new Word2Vec().setInputCol("tokenized_words").setOutputCol("features").setVectorSize(4).setMinCount(1)
    val out_df = word2Vec.fit(df)
    out_df
  }

  def vectorizeArtists(df:DataFrame): Word2VecModel = {
    val word2Vec = new Word2Vec().setInputCol("tokenized_words").setOutputCol("features").setVectorSize(4).setMinCount(1)
    val out_df = word2Vec.fit(df)
    out_df
  }

  def saveGenres(word2VecModel: Word2VecModel): Unit = {
    word2VecModel.write.overwrite().save(genresDir)
  }

  def saveArtists(word2VecModel: Word2VecModel): Unit = {
    word2VecModel.write.overwrite().save(artistsDir)
  }


  def loadGenres(): Word2VecModel = {
    Word2VecModel.load(genresDir)
  }

  def loadArtists(): Word2VecModel = {
    Word2VecModel.load(artistsDir)
  }



  def cosine(vec1: Array[Float], vec2: Array[Float]): Double = {
    assert(vec1.length == vec2.length, "Uneven vectors!")
    var dot, sum1, sum2 = 0.0
    for (i <- 0 until vec1.length) {
      dot += (vec1(i) * vec2(i))
      sum1 += (vec1(i) * vec1(i))
      sum2 += (vec2(i) * vec2(i))
    }
    dot / (math.sqrt(sum1) * math.sqrt(sum2))
  }


  def cosineArtists(word1: String, word2: String): Double = {
    assert(containsArtists(word1) && containsArtists(word2), "Out of dictionary word! " + word1 + " or " + word2)
    cosine(artistsVocab.get(word1).get, artistsVocab.get(word2).get)
  }

  def containsArtists(word: String): Boolean = {
    artistsVocab.get(word).isDefined
  }
}
