package Pipeline
import java.util.stream.Collectors

import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import DataTransform._
import MachineLearning._
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/*

class W2VPipeline(spark: SparkSession) extends  MainPipeline {

  val lyricsModelDirectoryPath = "/tmp/spark-logistic-regression-model"

  override def classify: CrossValidatorModel = throw new RuntimeException("Not supported for word2Vec")


  def train(): Unit = {
    val sentences = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("multiLine", true)
      .load("E:\\C drive\\NEU\\Scala\\Final\\datasets\\kaggle\\original.csv")// Give correct path here.

    val cleaner = new DataCleanerTransformer()
    val tokenizer = new WordTokenizerTransformer()
    val stopWordsRemover = new SWRemoverTransformer()

    val word2Vec = new Word2Vec().setInputCol(stopWordsRemover.outputCol).setOutputCol("features").setVectorSize(100).setMinCount(1)
    val pipeline = new Pipeline().setStages(Array(cleaner, tokenizer, stopWordsRemover, word2Vec))

    val model = pipeline.fit(sentences)
    saveModel(model, getPipelineModelDirectory())

    val word2VecModel = model.stages(model.stages.length -1)






    /*
    val numerator = new Nothing

    val tokenizer = new Nothing().setInputCol(CLEAN.getName).setOutputCol(WORDS.getName)
    val stopWordsRemover = new Nothing().setInputCol(WORDS.getName).setOutputCol(FILTERED_WORDS.getName)
    val exploder = new Nothing
    val stemmer = new Nothing
    val uniter = new Nothing
    val verser = new Nothing

    // Create model.
    val word2Vec = new Nothing().setInputCol(VERSE.getName).setOutputCol("features").setVectorSize(300).setMinCount(0)

    val pipeline = new Nothing().setStages(Array[Nothing](cleanser, numerator, tokenizer, stopWordsRemover, exploder, stemmer, uniter, verser, word2Vec))

    val pipelineModel = pipeline.fit(sentences)
    saveModel(pipelineModel, getModelDirectory)

    val modelStatistics = new Nothing
    val word2VecModel = pipelineModel.stages(pipelineModel.stages.length - 1).asInstanceOf[Nothing]

    modelStatistics.put("Word2Vec vectors count", word2VecModel.getVectors.count)

    */
  }

  def getPipelineModelDirectory(): String  = {
    lyricsModelDirectoryPath+"/word2vec/" + "/pipeline/"
  }


  def getWord2VecModelDirectory(): String  = {
    lyricsModelDirectoryPath+"/word2vec/" + "/word2vec/"
  }


  def findSynonyms(lyrics: String): DataFrame = {
    val pipelineModel = PipelineModel.read.load(getPipelineModelDirectory())
    val word2VecModel = Word2VecModel.load(getPipelineModelDirectory())
    val synonyms = word2VecModel.findSynonyms(lyrics, 5)
    return  synonyms
  }


}*/
