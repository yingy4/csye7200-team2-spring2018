package Pipeline

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.SparkSession

abstract class MainPipeline extends LyricsPipeline {

  val outputDir = "/tmp/spark-logistic-regression-model"

  override def predict(spark: SparkSession, unknownlyrics: String): String =  {
    val paragraphs = unknownlyrics.split("\\r?\\n{2,}")
    println(paragraphs.length)
    "test"
  }



  def saveModel(crossValidatorModel: Any, outputDir: String): Unit = crossValidatorModel match{
    case _: CrossValidatorModel=>println("cross validator model saved!")
    case _ :PipelineModel => println("pipeline model saved!")
    case _ => println(crossValidatorModel.getClass)
  }

}
