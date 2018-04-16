package DataTransform

import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import scala.util.matching.Regex

object DataCleaner {

  def clean(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("lyrics").isNotNull).withColumn("lyrics", regexp_replace(df("lyrics"), "[\\',\\n,\\t,\\r,\\r\\n,\\,]", ""))
    return dataframe
  }

}