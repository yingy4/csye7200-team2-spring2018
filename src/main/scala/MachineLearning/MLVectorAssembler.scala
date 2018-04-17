package MachineLearning

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

object MLVectorAssembler {

  def assemble(df: DataFrame): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("htffeatures", "fhfeatures"))
      .setOutputCol("features")

    val output = assembler.transform(df)
    /*println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    output.select("features", "clicked").show(false)*/
    output
  }
}
