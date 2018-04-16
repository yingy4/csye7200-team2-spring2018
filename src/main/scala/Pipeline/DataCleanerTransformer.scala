package Pipeline

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions.{col, regexp_replace, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

class DataCleanerTransformer extends Transformer {

  def clean(df:DataFrame): DataFrame = {
    val ret = df.where(df("lyrics").isNotNull).withColumn("lines",split(col("lyrics"), "\\n{1,}"))
    val dataframe = ret.where(ret("lyrics").isNotNull).withColumn("lyrics", regexp_replace(df("lyrics"), "[\\',\\n,\\t,\\r,\\r\\n,\\,]", ""))
    return dataframe
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = dataset.toDF()
    val ret = df.where(df("lyrics").isNotNull).withColumn("lines",split(col("lyrics"), "\\n{1,}"))
    val dataframe = ret.where(ret("lyrics").isNotNull).withColumn("lyrics", regexp_replace(df("lyrics"), "[\\',\\n,\\t,\\r,\\r\\n,\\,]", ""))
    return dataframe
  }

  override def copy(extra: ParamMap): Transformer = super.defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = return schema

  override val uid: String = "dc"
}
