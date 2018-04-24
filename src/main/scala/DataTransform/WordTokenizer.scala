package DataTransform


import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._



object WordTokenizer {
  /**Tokenize the lyrics into words.
    *
    * @param df Dataframe to transform
    * @param inColumnName Column name to transform
    * @param outColumnName Output column after transformation.
    * @return
    */
  def tokenize(df:DataFrame, inColumnName : String, outColumnName:String): DataFrame = {
    val regexTokenizer = new RegexTokenizer()
      .setInputCol(inColumnName)
      .setOutputCol(outColumnName)
      .setPattern("\\W")

    val out = regexTokenizer.transform(df)
    out
  }
}