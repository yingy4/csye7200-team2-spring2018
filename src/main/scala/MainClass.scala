


import DataTransform._
import FeatureExtraction.{ArtistsFrequency, GenreFrequency, ReadabilityScore, RhymeScheme, Utility}
import MachineLearning.Word2Vectorizer
import org.apache.spark.sql.SparkSession

import scala.util.Try

object MainClass {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("GenrePredictorFromLyrics")
      .master("local")
      .getOrCreate()


    val unknownlyrics = "This is me for forever\nOne of the lost ones\nThe one without a name\nWithout an honest heart as compass\n\nThis is me for forever\nOne without a name\nThese lines the last endeavor\nTo find the missing lifeline\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\n\nMy flower, withered between\nThe pages two and three\nThe once and forever bloom gone with my sins\nWalk the dark path\nSleep with angels\nCall the past for help\nTouch me with your love\nAnd reveal to me my true name\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nOnce and for all\nAnd all for once\nNemo my name forevermore\n\nNemo sailing home\nNemo letting go\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nOnce and for all\nAnd all for once\nNemo my name forevermore\n\nName for evermore"



    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("multiLine", true)
      .load("E:\\C drive\\NEU\\Scala\\Final\\datasets\\kaggle\\original.csv")// Give correct path here.
      //Rohan's path :E:\C drive\NEU\Scala\Final\datasets\kaggle\


    val cleanedData = DataCleaner.clean(df)
    val wordTokenizer = WordTokenizer.tokenize(cleanedData, "tokenized_words")
    val nonStpWordData = SWRemover.removeStopWords(wordTokenizer.where(wordTokenizer("lyrics").isNotNull))
    val swRemovedWordTokenizer = WordTokenizer.tokenize(nonStpWordData, "words")

 //   val artistsFrequency =ArtistsFrequency.filterByArtistFrequency(swRemovedWordTokenizer)
  //  val genresFrequency =GenreFrequency.filterByGenreFrequency(swRemovedWordTokenizer)

    val artistsGroup = ArtistsFrequency.groupLyrics(swRemovedWordTokenizer)
    val genresGroup = GenreFrequency.groupLyrics(swRemovedWordTokenizer)

    //val word2VecModelEntire = Word2Vectorizer.vectorize(swRemovedWordTokenizer)


    val word2VecModelGenres = Word2Vectorizer.vectorizeGenres(genresGroup)
    val word2VecModelArtists = Word2Vectorizer.vectorizeArtists(artistsGroup)


    val split = unknownlyrics.split("\\r?\\n{2,}")(0)
    word2VecModelGenres.findSynonyms(split, 1).show(false)




    word2VecModelArtists.findSynonyms(split, 1).show(false)





   // val w2vp = new W2VPipeline(spark).train()



    /*

    val cleanedData = DataCleaner.clean(df)
    val wordTokenizer = WordTokenizer.tokenize(cleanedData, "tokenized_words")
   // wordTokenizer.show(10, false)
    val nonStpWordData = SWRemover.removeStopWords(wordTokenizer.where(wordTokenizer("lyrics").isNotNull))
    val swRemovedWordTokenizer = WordTokenizer.tokenize(nonStpWordData, "words")

    val word2VecModel = Word2Vectorizer.vectorize(swRemovedWordTokenizer)


    word2VecModel.findSynonyms("cold", 1).show(false)
 //   val artistsFrequency = ArtistsFrequency.filterByArtistFrequency(swRemovedWordTokenizer)
 //   val artistsGroupedLyrics = ArtistsFrequency.groupLyrics(swRemovedWordTokenizer)
  //  val genreGroupedLyrics = GenreFrequency.groupLyrics(swRemovedWordTokenizer)


  //  artistsGroupedLyrics.show(false)
  //  genreGroupedLyrics.show(false)



    //val lyrics_ds = spark.createDataset(lyrics)





    //val model = new CrossValidatorModel()


  //  val crossValidatorModel = new CrossValidatorModel().classify()


    //val result = word2VecModel.transform(documentDF)
    //result.show(false)






  /*
    val documentDF = spark.createDataFrame(Seq(
      random.split(" ")

    ).map(Tuple1.apply)).toDF("tokenized_words")

    val result = word2VecModel.transform(documentDF)
    result.select("features").show(false)
  */

    //val artistsFrequency =ArtistsFrequency.filterByArtistFrequency(swRemovedWordTokenizer)
   // val genresFrequency =GenreFrequency.filterByGenreFrequency(swRemovedWordTokenizer)
   // genresFrequency.show(false)



  */

    // convert df to RDD(Tuple6)
    val rootRDD = df.rdd.map( row =>
      ( Try(row.getString(0).toInt),
        Try(row.getString(1)),
        Try(row.getString(2).toInt),
        Try(row.getString(3)),
        Try(row.getString(4)),
        Try(row.getString(5)) ))

    // transform with Readability Score feature
    val transformRDD1 = (rootRDD zip ReadabilityScore.transformWithScore(rootRDD)) map (Utility.flattenNestedTuple6)

    // transform with Rhyme Scheme feature
    val transformRDD2 = (transformRDD1 zip RhymeScheme.transformWithRhymeScheme(rootRDD)) map (Utility.flattenNestedTuple7)

  }

}

