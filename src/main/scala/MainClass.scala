


import DataTransform._
import FeatureExtraction._
import MachineLearning.Word2Vectorizer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.ListMap
import scala.util.Try

object MainClass {
  //val lyricsModelDirectoryPath = "/tmp/spark-logistic-regression-model/final/"
  val lyricsModelDirectoryPath = "/tmp/spark-logistic-regression-model/final/test/test2/"
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

    val access_key = args.head
    val secret_key = args(1)
    val runMode = args(2)
    
    val conf = new SparkConf()
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.hadoop.fs.s3a.access.key", access_key)
      .set("spark.hadoop.fs.s3a.secret.key", secret_key)

    if(!args.isEmpty && runMode.equals("local")) {

      // application running locally
      val spark = SparkSession
        .builder()
        .appName("GenrePredictorFromLyrics")
        .config(conf)
        .master("local")
        .getOrCreate()


      import scala.util.control.Breaks.{break, breakable}
      breakable {
        while (1 == 1) {
          println("1 -> Train, 2 -> Predict, 3 -> End")
          val inp = scala.io.StdIn.readInt()
          println(s"User input is $inp")
          if (inp == 3) break

          // call train or testpredict method
          if (inp == 1)
            train(spark, loadFileIntoDF(spark, "TrainLocal"))
          else
            testpredict(spark, loadFileIntoDF(spark, "PredictLocal"))

        }
      }

    } else {
      // application running on EMR

      val spark = SparkSession
        .builder()
        .appName("GenrePredictorFromLyrics")
        .config(conf)
        .getOrCreate()
      
      // call train method
      train(spark, loadFileIntoDF(spark, "TrainEMR"))

    }



  }

  def loadFileIntoDF(spark : SparkSession, str: String) : DataFrame = {

    if (str.equals("TrainLocal")) {

      val dataf = spark.read
        .format("csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .option("multiLine", true)
        .load("E:\\C drive\\NEU\\Scala\\Final\\datasets\\kaggle\\test.csv")// Give correct path here.
      //Rohan's path :E:\C drive\NEU\Scala\Final\datasets\kaggle\
      dataf

    } else if (str.equals("TrainEMR")){

      //.format("csv")
      val df = spark.read
        .option("header", "true") //reading the headers
        .option("multiLine", true)
        .option("mode", "DROPMALFORMED")
        .csv("s3a://sparkprojectbucket/lyrics.csv")

      return df
    } else {

      val datafr = spark.read
        .format("csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .option("multiLine", true)
        .load("E:\\C drive\\NEU\\Scala\\Final\\datasets\\kaggle\\test.csv")

      return datafr

    }


  }


  def train(spark: SparkSession, dataf : DataFrame) ={


  //  dataf.show(true)
    // extract Readability Score and Rhyme Scheme
    val transformedDF = extractRSAndRhymeScheme(dataf, spark)
    //transformedDF.show(2, false)
 //   transformedDF.show(true)
    println("End of RS and Rhyme Scheme Extraction.")

    // clean, tokenize, remove stop words from lyrics column
    val clean_lyrics  = extractFeaturesForLyrics(spark, transformedDF)


    println("End Lyrics Feature Extraction.")
    clean_lyrics.show(true)

    trainCrossValidatorModel(clean_lyrics)



//    clean_lyrics.show(1, false)

    // calling Word2Vec Pipeline
  //  trainWord2VecModel(clean_lyrics, spark.sparkContext)

    // calling CrossValidatorModel Pipeline
    //trainCrossValidatorModel(clean_lyrics)





    //val artistsFrequency =ArtistsFrequency.filterByArtistFrequency(clean_lyrics)
    //val genresFrequency =GenreFrequency.filterByGenreFrequency(clean_lyrics)

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


  }

  def trainWord2VecModel(clean_lyrics : DataFrame, spark:SparkContext) = {

    // Word2Vec pipeline start
    clean_lyrics.show(true)
    val genresGroup = GenreFrequency.groupLyrics(clean_lyrics)

    // train word2vec and return model
    val word2VecModelGenres = Word2Vectorizer.vectorizeGenres(genresGroup)

    //val word2VecModelSaveDir = lyricsModelDirectoryPath + "/word2vec/word2vec/fullcsv/finalpresentation/"
    val lyricsModelDirectoryPath = "/tmp/spark-logistic-regression-model/final/test/test2/"

    word2VecModelGenres.save(spark, lyricsModelDirectoryPath)

  }

  def trainCrossValidatorModel(clean_lyrics : DataFrame) = {

    // Configure an ML pipeline, which consists of three stages: hasher, tokenizer, hashingTF, assembler, and lr.
    val hasher = new FeatureHasher()
      .setInputCols("rs1", "rs2", "top1" , "top2" , "top3")
      .setOutputCol("features")
      .setCategoricalCols(Array("rs2", "top1" , "top2" , "top3"))

    val tokenizer = new Tokenizer()
      .setInputCol("clean_lyrics")
      .setOutputCol("pipeline_tokenized_words")

    /*

    val hashingTF = new HashingTF()
      //.setInputCol(tokenizer.getOutputCol)
      .setInputCol("filtered lyrics")
      .setOutputCol("htffeatures")

    val assembler = new VectorAssembler()
      .setInputCols(Array("htffeatures", "fhfeatures"))
      .setOutputCol("features")

*/
    val indexer = new StringIndexer().setInputCol("genre").setOutputCol("label").fit(clean_lyrics).setHandleInvalid("skip")
    // indexed.show(false)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setLabelCol("label")

    val converter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predicted genre")
      .setLabels(indexer.labels)





    val hasherout = hasher.transform(clean_lyrics)
    hasherout.show(true)



    //val tokenizer_out = tokenizer.transform(hasherout)
    val indexerout = indexer.transform(hasherout)
    indexerout.show(true)

  //  val lrout = lr.fit(indexerout)

    println("LR Model fit done.")

    val pipeline = new Pipeline()
      .setStages(Array(lr, converter))

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.3, 0.01))
      .build()


    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)  // Use 3+ in practice
      .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel



    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(indexerout)
    //cvModel.save(spark.sparkContext,"s3://...file_path.../")

    println("CV Model fit")
    cvModel.save("")
    println("CV Model fit Saved.")



    //Commented for testing without pipeline
     /*

    val pipeline = new Pipeline()
      .setStages(Array(hasher, indexer, lr, converter))

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
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




  //  val cvModelSaveDir = lyricsModelDirectoryPath + "/word2vec/cvmodel/finalpresentation/"
  //  cvModel.write.overwrite().save(cvModelSaveDir)





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


    println("count: " + rootRDD.count())
    // transform with Readability Score feature
    val transformRDD1 = (rootRDD zip ReadabilityScore.transformWithScore(rootRDD)) map (Utility.flattenNestedTuple6)

    println("Transform1rdd count: " + transformRDD1.count())

    for (t <- transformRDD1) {
      for {t1 <- t._1
           t2 <- t._2
           t3 <- t._3
           t4 <- t._4
           t5 <- t._5
           t6 <- t._6
           t7 <- t._7
           //t8 <- t._8
      } {
        println(t1 + " " + t2 + " " + t3 + " " + t4 + " " + t5 + " " + t6 + " " + t7 + " ")
      }
    }
    // transform with Rhyme Scheme feature
    val transformRDD2 = (transformRDD1 zip RhymeScheme.transformWithRhymeScheme(rootRDD)) map (Utility.flattenNestedTuple7)
    println("Transform2RDD count: " + transformRDD2.count())




    val transformRDD3 = for {t <- transformRDD2 if(t._1.isSuccess &&
                                                   t._2.isSuccess &&
                                                   t._3.isSuccess &&
                                                   t._4.isSuccess &&
                                                   t._5.isSuccess &&
                                                   t._6.isSuccess &&
                                                   t._7.isSuccess &&
                                                   t._8.isSuccess)}
    yield { Row(t._4.get, t._5.get, t._6.get, t._7.get, t._8.get) }

    println("Transform3 rdd count: " + transformRDD3.count())

    val schema = StructType(
        StructField("artist", StringType, false) ::
        StructField("genre", StringType, false) ::
        StructField("lyrics", StringType, false) ::
        StructField("rs1", DoubleType, false) ::
        StructField("rs2", StringType, false) :: Nil
    )

    val transformedDF = spark.createDataFrame(transformRDD3, schema)

    transformedDF

  }


  def extractFeaturesForLyrics(spark:SparkSession, transformedDF : DataFrame) : DataFrame = {
    transformedDF.show(5,true)
    val cleanedData = DataCleaner.cleanTrain(transformedDF)
    cleanedData.show(true)
    println("added clean_lyrics col")
    val wordTokenizer = WordTokenizer.tokenize(cleanedData,"clean_lyrics","clean_tokens")
    wordTokenizer.show(true)
    println("added clean_tokens col")
    //val wordTokenizer2 = WordTokenizer.tokenize(wordTokenizer,"clean_lyrics2","tokenized_words")
    //wordTokenizer2.show(5,true)
    //val nonStpWordData = SWRemover.removeStopWords(wordTokenizer.where(wordTokenizer("clean_lyrics").isNotNull))
    //println("added filtered_lyrics col")
    /*val swRemovedWordTokenizer = WordTokenizer.tokenize(nonStpWordData,"filtered lyrics","words")
    swRemovedWordTokenizer.show(5,true)*/
    val songTopWords = SongTokenizer.tokenizeSongs(spark, wordTokenizer)
    songTopWords.show(true)
    println("added top word cols")

    //val clean_lyrics = DataCleaner.cleanTrain(songTopWords)
    //clean_lyrics.show(5, true)
    //val clean = DataCleaner.cleanGenre(clean_lyrics)

    return songTopWords
  }


/*
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

*/

  def testpredict(spark:SparkSession, dataf: DataFrame ) = {


    /*val input = spark.createDataFrame(Seq(
      (6L, unknownlyrics, "unknown")
    )).toDF("id", "clean_lyrics", "genre")*/

    predictWord2Vec(spark.sparkContext)


/*
    // extracting Reading Score and Rhyme Scheme
    val rsAndRhymeScheme = extractRSAndRhymeScheme(dataf, spark)

    // Cleaning, tokenizing, stop word removing, Extracting Top words by features
    val lyricsFeatures = extractFeaturesForLyrics(rsAndRhymeScheme)


    predictWord2Vec(spark.sparkContext)

    //Directory to load the saved model from
    val cvModelSaveDir = lyricsModelDirectoryPath + "/word2vec/cvmodel/finalpresentation/"

    //Load the saved model
    val cvModel = CrossValidatorModel.load(cvModelSaveDir)

    //Run prediction
    val predictedDF = cvModel.transform(lyricsFeatures)

    //Print the output
      predictedDF
      .select("genre",  "probability", "predicted genre")
      .collect()
      .foreach { case Row(genre: String, prob: Vector, prediction: Double) =>
        println(s"Actual genre = $genre --> prob=$prob, prediction=$prediction")
      }
  */
  }


  def predictWord2Vec(spark: SparkContext): Unit = {
    // sample test word2vec model start
    //val word2VecModelSaveDir = lyricsModelDirectoryPath + "/word2vec/word2vec/fullcsv/finalpresentation/"

    //Trained
    //val word2VecModelSaveDir = lyricsModelDirectoryPath + "/word2vec/word2vec/finalpresentation/"
    val lyricsModelDirectoryPath = "/tmp/spark-logistic-regression-model/final/test/test2/"

    val word2VecModelGenres = Word2VecModel.load(spark, lyricsModelDirectoryPath)
    val unknownlyrics2 = "My heart is sad and lonely\nFor you I sigh, for you dear only\nWhy haven't you seen it\nI'm all for you body and soul\nI spend my days in longing\nAnd wondering why it's me you're wronging\nI tell you I mean it\nI'm all for you body and soul\nI can't believe it\nIt's hard to conceive it\nThat you'd turn away romance\nAre you pretending\nIt looks like the ending\nUnless I could have just one more chance to prove, dear\nMy life a wreck you're making\nYou know I'm yours for just the taking\nI'd gladly surrender myself to you body and soul"
    val splitSentences = unknownlyrics2.split("\\r?\\n{1,}")
    val splitWords = unknownlyrics2.replaceAll("\\n"," ").trim.split("\\s+")


    // creating map of genres and their respective vectors
    val list = List("Pop" , "Rock", "Electronic", "Hip-Hop", "Metal", "Jazz", "Folk", "R&B",  "Other", "Country", "Indie")
    val map = list.foldLeft(ListMap[String, Array[Double]]()) {
      (acc, genre)=> {
        acc + (genre->word2VecModelGenres.transform(genre).toArray)
      }
    }

    // creating Array[Try[Vector]] for genres to vectors
    val vta = for(word <- splitWords) yield Try(word2VecModelGenres.transform(word))

    // removing failed trys
    val sa = for (vt <- vta if (vt.isSuccess)) yield findSimilarGenre(vt.get, map)

    // getting best or most frequently occurring genres for vectors of all words
    val distinct = sa.distinct
    val bestGenre = distinct.foldLeft(("",0)){(acc, genre) => {
      val c = sa.count(_.equals(genre))
      if (c > acc._2) (genre,c) else (acc._1,acc._2)
    }}

    val predictedGenre = bestGenre._1
    println("Predicted Genre using Word2Vec Pipeline: " + predictedGenre)
    // W2V Sample Testing Ends
  }

  /** Find similarity between 2 words by using their cosine similarity.
    *
    *
    * @param unknownwordVector
    * @param map
    * @return
    */
  def findSimilarGenre(unknownwordVector: Vector, map : Map[String,Array[Double]]): String = {
    val closestGenre = map.foldLeft(("",0.0 )) {(acc, kv) => {
      val similarity = Word2Vectorizer.cosine(unknownwordVector.toArray, kv._2)
      if(acc._2 > similarity)
        acc
      else
        (kv._1, similarity)
    }}

    closestGenre._1
  }
}

