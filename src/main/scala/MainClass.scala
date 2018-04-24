


import DataTransform._
import FeatureExtraction._
import MachineLearning.Word2Vectorizer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.immutable.ListMap
import scala.util.Try

object MainClass {
  val lyricsModelDirectoryPath = "src\\main\\resources\\Model\\lyricsModelDirectoryPath"
  val lyricsModelArtistDirectoryPath = "src\\main\\resources\\Model\\lyricsModelArtistDirectoryPath"
  val lyricsW2VModelDirectoryPath = "src\\main\\resources\\W2VModel\\lyricsModelDirectoryPath"

  def main(args: Array[String]): Unit = {
    val runMode = Try(args.head)
    val access_key= Try(args(1))
    val secret_key = Try(args(2))
    


    if(runMode.isSuccess && runMode.get.equals("local")) {

      // application running locally
      val spark = SparkSession
        .builder()
        .appName("GenrePredictorFromLyrics")
        .master("local")
        .getOrCreate()


      import scala.util.control.Breaks.{break, breakable}
      breakable {
        while (1 == 1) {
          println("1 -> Train, 2 -> Predict, 3 -> End")
          val inp = scala.io.StdIn.readInt()
          println(s"User input is $inp")

          println("1 -> Genre, 2 -> Artist, 3 -> End")
          val inp2 = scala.io.StdIn.readInt()
          println(s"User input is $inp2")

          if (inp == 3 || inp2 == 3) break

          val label = if (inp2 == 1) "genre" else "artist"

          // call train or predict method
          if (inp == 1)
              train(spark, loadFileIntoDF(spark, "TrainLocal"), label)
          else
            predict(spark, loadFileIntoDF(spark, "PredictLocal"), label)
        }
      }

    } else {
      // application running on EMR
    if(access_key.isSuccess && secret_key.isSuccess && runMode.isSuccess) {

      val conf = new SparkConf()
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.access.key", access_key.get)
        .set("spark.hadoop.fs.s3a.secret.key", secret_key.get)

      val spark = SparkSession
        .builder()
        .appName("GenrePredictorFromLyrics")
        .config(conf)
        .getOrCreate()

      // call train method
      train(spark, loadFileIntoDF(spark, "TrainEMR"), "genre")
    } else {
      println("Provided program arguments are not correct. \n " +
              "Specify 'local' if running locally. \n" +
              "Specify 'emr <access_key> <secret_key> if running on EMR")
    }

    }
  }

  /**Load the respective csv files for respective run modes.
    *
    *
    * @param spark sparkSession
    * @param str Run mode: TrainEMR: EMR mode, TrainLocal: Local mode, PredictLocal: Predition Mode
    * @return Dataframe of the loaded csv
    */
  def loadFileIntoDF(spark : SparkSession, str: String) : DataFrame = {

    if (str.equals("TrainLocal")) {

      val dataf = spark.read
        .format("csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .option("multiLine", true)
        .load("src\\main\\resources\\train.csv")
      dataf

    } else if (str.equals("TrainEMR")){
      val df = spark.read
        .option("header", "true") //reading the headers
        .option("multiLine", true)
        .option("mode", "DROPMALFORMED")
        .csv("s3a://sparkprojectbucket/test.csv")

      return df
    } else {

      val datafr = spark.read
        .format("csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .option("multiLine", true)
        .load("src\\main\\resources\\test.csv")
      return datafr
    }
  }


  /** Train the Cross Validator Model for the loaded csv dataframe.
    *
    * @param spark sparkSession
    * @param dataf dataframe to load and train the model.
    */
  def train(spark: SparkSession, dataf : DataFrame, label: String) ={
    // extract Readability Score and Rhyme Scheme
    val transformedDF = extractRSAndRhymeScheme(dataf, spark)
    println("End of RS and Rhyme Scheme Extraction.")

    // clean, tokenize, remove stop words from lyrics column
    val clean_lyrics  = extractFeaturesForLyrics(spark, transformedDF)


    println("End Lyrics Feature Extraction.")

    //Training the Cross Validator Model.
    trainCrossValidatorModel(clean_lyrics, label)


    // calling Word2Vec Pipeline. Uncomment to train Word2Vec Model.
  //  trainWord2VecModel(clean_lyrics, spark.sparkContext)


    //Get Top words by artists
    //val artistsFrequency =ArtistsFrequency.filterByArtistFrequency(clean_lyrics)

    //Get Top words by genres
    //val genresFrequency =GenreFrequency.filterByGenreFrequency(clean_lyrics)
  }

  /** Train the Word2Vec model and save it to the directory mentioned.
    *
    *
    * @param clean_lyrics dataframe
    * @param spark sparkSession
    */
  def trainWord2VecModel(clean_lyrics : DataFrame, spark:SparkContext) = {
    // Word2Vec pipeline start
    clean_lyrics.show(true)
    val genresGroup = GenreFrequency.groupLyrics(clean_lyrics)

    // train word2vec and return model
    val word2VecModelGenres = Word2Vectorizer.vectorizeGenres(genresGroup)
    word2VecModelGenres.save(spark, lyricsW2VModelDirectoryPath)
  }


  /** Train the Cross Validator Model and save it to the directory specified for it.
    *
    *
    * @param clean_lyrics dataframe to train the model.
    */
  def trainCrossValidatorModel(clean_lyrics : DataFrame, label: String) = {

    // Configure an ML pipeline, which consists of three stages: hasher, indexer,  lr and converter.
    val hasher = new FeatureHasher()
      .setInputCols("rs1", "rs2", "top1" , "top2" , "top3")
      .setOutputCol("features")
      .setCategoricalCols(Array("rs2", "top1" , "top2" , "top3"))

    val indexer = new StringIndexer().setInputCol(label).setOutputCol("label").fit(clean_lyrics).setHandleInvalid("skip")
    // indexed.show(false)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setLabelCol("label")

    val converter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol(s"predicted $label")
      .setLabels(indexer.labels)

    val hasherout = hasher.transform(clean_lyrics)

    val indexerout = indexer.transform(hasherout)

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
    val cvModel: CrossValidatorModel = cv.fit(indexerout)

    println("CV Model fit")
    if (label.equals("genre"))
      cvModel.write.overwrite().save(lyricsModelDirectoryPath)
    else
      cvModel.write.overwrite().save(lyricsModelArtistDirectoryPath)
    println("CV Model fit Saved.")
  }


  /**
    * Extrating Rhyme Scheme and Reading Score as rs1 and rs2 column.
    *
    *
    * @param df dataframe
    * @param spark sparkSession
    * @return Dataframe with Rhyme Scheme and Reading Score as rs1 and rs2 column.
    */
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
    yield { Row(t._4.get, t._5.get, t._6.get, t._7.get, t._8.get) }


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


  /**Extracting the Features by cleaning, tokenizing the lyrics and getting top words by song.
    *
    *
    * @param spark SparkSession
    * @param transformedDF Dataframe to extract features from.
    * @return Dataframe with cleaned, tokenized and top words extracted for each song.
    */
  def extractFeaturesForLyrics(spark:SparkSession, transformedDF : DataFrame) : DataFrame = {
    transformedDF.show(5,true)
    val cleanedData = DataCleaner.cleanTrain(transformedDF)
    val wordTokenizer = WordTokenizer.tokenize(cleanedData,"clean_lyrics","clean_tokens")
    val songTopWords = SongTokenizer.tokenizeSongs(spark, wordTokenizer)
    songTopWords
  }


  /** Run prediction on the trained model.
    *
    * @param spark SparkSession
    * @param dataf The csv to be predicted transformed as Dataframe.
    */
  def predict(spark:SparkSession, dataf: DataFrame, label: String ) = {
    val rsAndRhymeScheme = extractRSAndRhymeScheme(dataf, spark)

    // Cleaning, tokenizing, stop word removing, Extracting Top words by features
    val lyricsFeatures = extractFeaturesForLyrics(spark, rsAndRhymeScheme)

    val hasher = new FeatureHasher()
      .setInputCols("rs1", "rs2", "top1" , "top2" , "top3")
      .setOutputCol("features")
      .setCategoricalCols(Array("rs2", "top1" , "top2" , "top3"))
    val hasherout = hasher.transform(lyricsFeatures)

    predictCrossValidation(hasherout, label)
  }


  /** Get the trained model and run prediction on the datafarme provided for testing.
    *
    * @param spark Spark Session
    */
  def predictWord2Vec(spark: SparkContext): Unit = {

    //Trained

    val word2VecModelGenres = Word2VecModel.load(spark, lyricsW2VModelDirectoryPath)
    val unknownlyrics = "My heart is sad and lonely\nFor you I sigh, for you dear only\nWhy haven't you seen it\nI'm all for you body and soul\nI spend my days in longing\nAnd wondering why it's me you're wronging\nI tell you I mean it\nI'm all for you body and soul\nI can't believe it\nIt's hard to conceive it\nThat you'd turn away romance\nAre you pretending\nIt looks like the ending\nUnless I could have just one more chance to prove, dear\nMy life a wreck you're making\nYou know I'm yours for just the taking\nI'd gladly surrender myself to you body and soul"
    val splitSentences = unknownlyrics.split("\\r?\\n{1,}")
    val splitWords = unknownlyrics.replaceAll("\\n"," ").trim.split("\\s+")


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


  /**Load and Run prediction on the dataframe using the Cross Validator model that has been trained and saved  in the method above.
    *
    * @param df Dataframe of the lyrics to be predicted.
    */
  def predictCrossValidation(df: DataFrame, label: String) = {
    val cvModel = if (label.equals("genre"))
      CrossValidatorModel.load(lyricsModelDirectoryPath)
    else
      CrossValidatorModel.load(lyricsModelArtistDirectoryPath)

    //Run prediction
    val predictedDF = cvModel.transform(df)
    println("The prediction result for the provided dataframe is aas follows:")
    predictedDF.select("artist", "genre", "rawPrediction", "probability", s"predicted $label").show(true)
  }
}

