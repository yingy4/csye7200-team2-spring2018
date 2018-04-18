


import DataTransform._
import FeatureExtraction._
import MachineLearning.Word2Vectorizer
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try

object MainClass {
  //val lyricsModelDirectoryPath = "/tmp/spark-logistic-regression-model/final/"
  val lyricsModelDirectoryPath = "/tmp/spark-logistic-regression-model/final/test/"
  /*
  Id    Genre
  0.0 - Pop
  1.0 - Rock
  2.0 - Electronic
  3.0 - Hip-Hop
  4.0 - Metal
  5.0 - Jazz
  6.0 - Folk
  7.0 - NA
  8.0 - RB
  9.0 - Other
  10.0 - Country
  11.0 - Indie
   */

  def main(args: Array[String]): Unit = {

    if(!args.isEmpty && args.head.equals("local")) {
      // application running locally
      val spark = SparkSession
        .builder()
        .appName("GenrePredictorFromLyrics")
        .master("local")
        .getOrCreate()


      /*
          val unknownlyrics = "This is me for forever\nOne of the lost ones\nThe one without a name\nWithout an honest heart as compass\n\nThis is me for forever\nOne without a name\nThese lines the last endeavor\nTo find the missing lifeline\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\n\nMy flower, withered between\nThe pages two and three\nThe once and forever bloom gone with my sins\nWalk the dark path\nSleep with angels\nCall the past for help\nTouch me with your love\nAnd reveal to me my true name\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nOnce and for all\nAnd all for once\nNemo my name forevermore\n\nNemo sailing home\nNemo letting go\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nOnce and for all\nAnd all for once\nNemo my name forevermore\n\nName for evermore"


         // train(spark)
         // predict(spark, unknownlyrics)

          testtrain(spark)
          testpredict(spark, unknownlyrics)
          //predict(spark, unknownlyrics)
      */

      // Converting Readability Score and Rhyme Scheme to feature vectors

      // train(spark)
      //  println("----------------------------------------Training Completed.--------------------------------------------------------------------")
      //  testpredict(spark, unknownlyrics)

      val unknownlyrics = "This is me for forever\nOne of the lost ones\nThe one without a name\nWithout an honest heart as compass\n\nThis is me for forever\nOne without a name\nThese lines the last endeavor\nTo find the missing lifeline\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\n\nMy flower, withered between\nThe pages two and three\nThe once and forever bloom gone with my sins\nWalk the dark path\nSleep with angels\nCall the past for help\nTouch me with your love\nAnd reveal to me my true name\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nOnce and for all\nAnd all for once\nNemo my name forevermore\n\nNemo sailing home\nNemo letting go\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nOnce and for all\nAnd all for once\nNemo my name forevermore\n\nName for evermore"

      import scala.util.control.Breaks.{break, breakable}
      breakable {
        while (1 == 1) {
          println("1 -> Train, 2 -> Predict, 3 -> End")
          val inp = scala.io.StdIn.readInt()
          println(s"User input is $inp")
          if (inp == 3) break

          // call train or testpredict method
          if (inp == 1) train(spark) else testpredict(spark, unknownlyrics)

        }
      }

    } else {
      // application running on EMR
      import GitIgnoredMethods._
      val conf = setSparkConfWithAccessKey(new SparkConf()
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))

      val spark = SparkSession
        .builder()
        .appName("GenrePredictorFromLyrics")
        .config(conf)
        .getOrCreate()

      val df = spark.read
        .option("header", "true")
        .option("multiLine", true)
        .option("mode", "DROPMALFORMED")
        .csv("s3a://sparkprojectbucket/lyrics.csv")

      // call train method


      df.show(10, true)
    }



  }


  def train(spark: SparkSession) ={
    //Load csv as DataFrame.
    val dataf = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("multiLine", true)
      .load("E:\\C drive\\NEU\\Scala\\Final\\datasets\\kaggle\\original.csv")// Give correct path here.
    //Rohan's path :E:\C drive\NEU\Scala\Final\datasets\kaggle\
    //C:\Users\kunal\Documents\Scala\Scala project\csye7200-team2-spring2018\src\main\resources\subset.csv


    //val df = dataf.limit(20)

    // extract Readability Score and Rhyme Scheme
    val transformedDF = extractRSAndRhymeScheme(dataf, spark)

    println("End of RS and Rhyme Scheme Extraction.")


    val clean_lyrics  = extractFeaturesForLyrics(transformedDF)

    println("End Lyrics Feature Extraction.")




    // W2V Testing Starts
//    val word2VecModel = Word2Vectorizer.vectorize(clean_lyrics)
    val genresGroup = GenreFrequency.groupLyrics(clean_lyrics)
    val word2VecModelGenres = Word2Vectorizer.vectorizeGenres(genresGroup)



    val unknownlyrics2 = "This is me for forever\nOne of the lost ones\nThe one without a name\nWithout an honest heart as compass\n\nThis is me for forever\nOne without a name\nThese lines the last endeavor\nTo find the missing lifeline\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\n\nMy flower, withered between\nThe pages two and three\nThe once and forever bloom gone with my sins\nWalk the dark path\nSleep with angels\nCall the past for help\nTouch me with your love\nAnd reveal to me my true name\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nOnce and for all\nAnd all for once\nNemo my name forevermore\n\nNemo sailing home\nNemo letting go\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nOnce and for all\nAnd all for once\nNemo my name forevermore\n\nName for evermore"
    val splitSentences = unknownlyrics2.split("\\r?\\n{1,}")
    val splitWords = unknownlyrics2.split("\\s+")
    def Sentences =
      for {
        sentence <- splitSentences
      } yield (println(sentence))



    val cleanedData = DataCleaner.cleanPredict(spark.createDataFrame(Seq(
      (0, unknownlyrics2)
    )).toDF("id", "lyrics"))

    cleanedData.show(false)

/*
    println("--------------------cosinesimilarity-----------------------")
    for(word <- splitWords) {
      println("-------------------------------------------------------------------------")
      println(word)
      try {
        val synonyms = word2VecModelGenres.findSynonyms("word", 5)

        for((synonym, cosineSimilarity) <- synonyms) {
          println(s"$synonym $cosineSimilarity")
        }

      }catch {
        case e: IllegalStateException => print("")
      }
      println("-------------------------------------------------------------------------")
    }
*/


    // W2V Testing Ends

    //testtrain(spark, songTopWords)

    /*

    val artistsFrequency =ArtistsFrequency.filterByArtistFrequency(songTopWords)
    val genresFrequency =GenreFrequency.filterByGenreFrequency(songTopWords)

    artistsFrequency.show(false)
    genresFrequency.show(false)
*/

  //  val artistsGroup = ArtistsFrequency.groupLyrics(swRemovedWordTokenizer)
  //  val genresGroup = GenreFrequency.groupLyrics(swRemovedWordTokenizer)


    //artistsGroup.
   // GenreFrequency.addGenre(genresGroup)


    //val word2VecModelEntire = Word2Vectorizer.vectorize(swRemovedWordTokenizer)

 //   val word2VecModelGenres = Word2Vectorizer.vectorizeGenres(genresGroup)
   // val word2VecModelArtists = Word2Vectorizer.vectorizeArtists(artistsGroup)

  //  Word2Vectorizer.saveArtists(word2VecModelArtists)
  //  Word2Vectorizer.saveGenres(word2VecModelGenres)

   //
   // Word2Vectorizer.cosineArtists("")

 //   println("saved")


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







    println("--------------------------------------------------------------------------------------------------------------------------------------------------------")
    //clean.show(false)
    println("--------------------------------------------------------------------------------------------------------------------------------------------------------")

    val unknownlyrics = "This is me for forever\nOne of the lost ones\nThe one without a name\nWithout an honest heart as compass\n\nThis is me for forever\nOne without a name\nThese lines the last endeavor\nTo find the missing lifeline\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\n\nMy flower, withered between\nThe pages two and three\nThe once and forever bloom gone with my sins\nWalk the dark path\nSleep with angels\nCall the past for help\nTouch me with your love\nAnd reveal to me my true name\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nOnce and for all\nAnd all for once\nNemo my name forevermore\n\nNemo sailing home\nNemo letting go\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nOnce and for all\nAnd all for once\nNemo my name forevermore\n\nName for evermore"


  /* Testing Starts
    // Configure an ML pipeline, which consists of three stages: hasher, tokenizer, hashingTF, assembler, and lr.
    val hasher = new FeatureHasher()
      .setInputCols("rs1", "rs2")
      .setOutputCol("fhfeatures")
      .setCategoricalCols(Array("rs2"))

    val tokenizer = new Tokenizer()
      .setInputCol("clean_lyrics")
      .setOutputCol("pipeline_tokenized_words")

    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("htffeatures")

    val assembler = new VectorAssembler()
      .setInputCols(Array("htffeatures", "fhfeatures"))
      .setOutputCol("features")

    val indexer = new StringIndexer().setInputCol("genre").setOutputCol("genre_code").fit(clean_lyrics).setHandleInvalid("skip")
    // indexed.show(false)

    val lr = new LogisticRegression()
      .setMaxIter(1).setLabelCol("genre_code")

    val converter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predicted genre")
      .setLabels(indexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(hasher, tokenizer, hashingTF, assembler, indexer, rf, converter))

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator().setLabelCol("genre_code"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)  // Use 3+ in practice
      .setParallelism(4)  // Evaluate up to 2 parameter settings in parallel

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(clean_lyrics)

    Testing Ends
    */


    /*

        // Prepare test documents, which are unlabeled (id, text) tuples.
        val test = spark.createDataFrame(Seq(
          (6L, unknownlyrics)
        )).toDF("id", "clean_lyrics")

        // Make predictions on test documents. cvModel uses the best model found (lrModel).
        cvModel.transform(test)
          .select("id", "clean_lyrics", "probability", "prediction")
          .collect()
          .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
            println(s"($id, $text) --> prob=$prob, prediction=$prediction")
          }
    */


    /* Testing Starts
   val cvModelSaveDir = lyricsModelDirectoryPath + "/word2vec/cvmodel/"
    cvModel.write.overwrite().save(cvModelSaveDir)
     Testing Ends
    */
  }



  def extractRSAndRhymeScheme(df: DataFrame, spark: SparkSession): DataFrame = {
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

    val transformRDD3 = for {t <- transformRDD2 if(t._1.isSuccess &&
                                                   t._2.isSuccess &&
                                                   t._3.isSuccess &&
                                                   t._4.isSuccess &&
                                                   t._5.isSuccess &&
                                                   t._6.isSuccess &&
                                                   t._7.isSuccess &&
                                                   t._8.isSuccess)}
    yield {
      val genre = t._5.get
      Row(t._1.get, t._2.get, t._3.get, t._4.get, t._5.get, t._6.get, t._7.get, t._8.get, t._6.get.replaceAll("[\\n]",s" $genre \n")) }


    val schema = StructType(
      StructField("index", IntegerType, false) ::
        StructField("song", StringType, false) ::
        StructField("year", IntegerType, false) ::
        StructField("artist", StringType, false) ::
        StructField("genre", StringType, false) ::
        StructField("lyrics", StringType, false) ::
        StructField("rs1", DoubleType, false) ::
        StructField("rs2", StringType, false) ::
        StructField("lyricsWithGenre", StringType, false) :: Nil
    )

    val transformedDF = spark.createDataFrame(transformRDD3, schema)

    transformedDF

  }


  def extractFeaturesForLyrics(transformedDF : DataFrame) : DataFrame = {
    val cleanedData = DataCleaner.cleanTrain(transformedDF)
    val wordTokenizer = WordTokenizer.tokenize(cleanedData, "tokenized_words")
    val nonStpWordData = SWRemover.removeStopWords(wordTokenizer.where(wordTokenizer("clean_lyrics").isNotNull))
    val swRemovedWordTokenizer = WordTokenizer.tokenize(nonStpWordData, "words")
    val songTopWords = SongTokenizer.tokenizeSongs(swRemovedWordTokenizer)

    val clean_lyrics = DataCleaner.cleanTrain(songTopWords)
    //val clean = DataCleaner.cleanGenre(clean_lyrics)

    return clean_lyrics
  }



  def predict(spark: SparkSession, unknownlyrics: String) = {
    val splitSentences = unknownlyrics.split("\\r?\\n{1,}")
    val splitWords = unknownlyrics.split("\\s+")

    val word2VecModelGenres = Word2Vectorizer.loadGenres()
    val word2VecModelArtists = Word2Vectorizer.loadArtists()

    def Sentences =
      for {
        sentence <- splitSentences
      } yield (println(sentence))



    val cleanedData = DataCleaner.cleanPredict(spark.createDataFrame(Seq(
      (0, unknownlyrics)
    )).toDF("id", "lyrics"))

    cleanedData.show(false)

    for(word <- splitWords) {
      println("-------------------------------------------------------------------------")
      println(word)
      try {
        word2VecModelGenres.findSynonyms(word.toLowerCase, 1).show(false)
      }catch {
        case e: IllegalStateException => print("")
      }
      println("-------------------------------------------------------------------------")
    }



/*
      for(words <- splitWords) {
        println(words)
        println("testing breaker!!")
      }

*/


    //println(split)


  //  val word2VecModelGenres = Word2Vectorizer.loadGenres()
  //  val word2VecModelArtists = Word2Vectorizer.loadArtists()


    //word2VecModelGenres.findSynonyms(split, 1).show(false)
    //word2VecModelArtists.findSynonyms(split, 1).show(false)
  }



  def testpredict(spark:SparkSession, unknownlyrics: String ) = {

    val dataf = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("multiLine", true)
      .load("E:\\C drive\\NEU\\Scala\\Final\\datasets\\kaggle\\test.csv")



    /*val input = spark.createDataFrame(Seq(
      (6L, unknownlyrics, "unknown")
    )).toDF("id", "clean_lyrics", "genre")*/


    val rsAndRhymeScheme = extractRSAndRhymeScheme(dataf, spark)
    val lyricsFeatures = extractFeaturesForLyrics(rsAndRhymeScheme)

    val cvModelSaveDir = lyricsModelDirectoryPath + "/word2vec/cvmodel/"

    val cvModel = CrossValidatorModel.load(cvModelSaveDir)


    val predictedDF = cvModel.transform(lyricsFeatures)

      predictedDF
      .select("genre",  "probability", "predicted genre")
      .collect()
      .foreach { case Row(genre: String, prob: Vector, prediction: Double) =>
        println(s"Actual genre = $genre --> prob=$prob, prediction=$prediction")
      }
  }
}

