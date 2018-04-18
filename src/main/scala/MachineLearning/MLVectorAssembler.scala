package MachineLearning

import java.util.UUID

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

class MLVectorAssembler  extends Transformer {

  override def transform(df: Dataset[_]): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("htffeatures", "fhfeatures"))
      .setOutputCol("features")



    val output = assembler.transform(df)
    output.show(false)
    /*println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    output.select("features", "clicked").show(false)*/
    output
  }

  override def copy(extra: ParamMap): Transformer = this

  override def transformSchema(schema: StructType): StructType = return schema

  override val uid: String = UUID.randomUUID().toString()
}
