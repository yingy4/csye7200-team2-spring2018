package Pipeline

import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.SparkSession


trait LyricsPipeline{
  def classify: CrossValidatorModel
  def predict(spark: SparkSession, unknownlyrics: String) : String



}
