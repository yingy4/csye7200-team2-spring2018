import DataTransform.WordTokenizer
import org.apache.spark.sql.SparkSession

object MainClass {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("GenrePredictorFromLyrics")
      .master("local")
      .getOrCreate()


    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/home/ubuntu/Downloads/lyrics.csv")


    val wordTokenizer = WordTokenizer.tokenize(df)


    println(wordTokenizer)

  }

}
