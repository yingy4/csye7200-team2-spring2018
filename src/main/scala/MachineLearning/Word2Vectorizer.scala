package MachineLearning

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.DataFrame

object Word2Vectorizer {
  val lyricsModelDirectoryPath = "/tmp/spark-logistic-regression-model"
  val genresDir = lyricsModelDirectoryPath+"/word2vec/" + "/word2vec/genres/"
  val artistsDir = lyricsModelDirectoryPath+"/word2vec/" + "/word2vec/artists/"

    def vectorize(df:DataFrame): Word2VecModel = {
      val word2Vec = new Word2Vec().setInputCol("words").setOutputCol("features").setVectorSize(3).setMinCount(1)
      val out_df = word2Vec.fit(df)
      out_df
    }

  def vectorizeGenres(df:DataFrame): Word2VecModel = {
    val word2Vec = new Word2Vec().setInputCol("tokenized_words").setOutputCol("features").setVectorSize(3).setMinCount(1)
    val out_df = word2Vec.fit(df)
    out_df
  }

  def vectorizeArtists(df:DataFrame): Word2VecModel = {
    val word2Vec = new Word2Vec().setInputCol("tokenized_words").setOutputCol("features").setVectorSize(3).setMinCount(1)
    val out_df = word2Vec.fit(df)
    out_df
  }

  def saveGenres(word2VecModel: Word2VecModel): Unit = {
    word2VecModel.save(genresDir)
  }

  def saveArtists(word2VecModel: Word2VecModel): Unit = {
    word2VecModel.save(artistsDir)
  }


  def loadGenres(): Word2VecModel = {
    Word2VecModel.load(genresDir)
  }

  def loadArtists(): Word2VecModel = {
    Word2VecModel.load(artistsDir)
  }

}
