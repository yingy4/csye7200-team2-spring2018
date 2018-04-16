package MachineLearning

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.DataFrame


object Word2Vectorizer {
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

}
