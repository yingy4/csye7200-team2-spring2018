import DataTransform._
import FeatureExtraction.{ArtistsFrequency, GenreFrequency}
import org.apache.spark.sql.SparkSession

object MainClass {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("GenrePredictorFromLyrics")
      .master("local")
      .getOrCreate()




   // val random = "This is me for forever One of the lost ones The one without a name Without an honest heart as compass This is me for forever One without a name These lines the last endeavor To find the missing lifeline"

    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("subset.csv")// Give correct path here.

    val cleanedData = DataCleaner.clean(df)
    val wordTokenizer = WordTokenizer.tokenize(cleanedData, "tokenized_words")
    wordTokenizer.show(10, false)
    val nonStpWordData = SWRemover.removeStopWords(wordTokenizer.where(wordTokenizer("lyrics").isNotNull))
    val swRemovedWordTokenizer = WordTokenizer.tokenize(nonStpWordData, "words")

    val artistsFrequency =ArtistsFrequency.filterByArtistFrequency(swRemovedWordTokenizer)
    val genresFrequency =GenreFrequency.filterByGenreFrequency(swRemovedWordTokenizer)
    genresFrequency.show(false)




  }

}
