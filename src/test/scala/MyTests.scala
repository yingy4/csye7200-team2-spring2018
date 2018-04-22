
import org.scalatest.FlatSpec
import FeatureExtraction.Utility._
import DataTransform._
import FeatureExtraction.Utility
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try

class MyTests extends FlatSpec{
  "ssml" should "have 4 occurences in smlssmllmslmssmlslssmlssml" in {
    assertResult(4) {searchSubstringOccurences("ssml", "smlssmllmslmssmlslssmlssml")}
  }

  "tokenizer " should "have 5 tokens " in {
    assertResult(5) {
      lazy val spark: SparkSession = {
        SparkSession
          .builder()
          .master("local")
          .appName("spark test example")
          .getOrCreate()
      }

      val test_df = spark.createDataFrame(Seq(
        (6L, " Testing Dataframes With Five Words")
      )).toDF("id", "lyrics")

      val tokens = WordTokenizer.tokenize(test_df, "lyrics" , "lyrics_tokens")
      //tokens.rdd.collect()(0)
      tokens.select("lyrics_tokens").limit(1).first().getSeq(0).length
    }
  }


  "tokenizer with lines" should "have 11 tokens " in {
    assertResult(11) {
      lazy val spark: SparkSession = {
        SparkSession
          .builder()
          .master("local")
          .appName("spark test example")
          .getOrCreate()
      }

      val test_df = spark.createDataFrame(Seq(
        (6L, " Testing Dataframes With Five Words\\n Testing Dataframes With Five Words")
      )).toDF("id", "lyrics")

      val tokens = WordTokenizer.tokenize(test_df, "lyrics" , "lyrics_tokens")
      //tokens.rdd.collect()(0)
      tokens.select("lyrics_tokens").limit(1).first().getSeq(0).length
    }
  }

  "swremoval  with Stop Words " should "have 13 tokens  without removal and 7 after removal " in {
    assertResult(7) {
      lazy val spark: SparkSession = {
        SparkSession
          .builder()
          .master("local")
          .appName("spark test example")
          .getOrCreate()
      }

      val test_df = spark.createDataFrame(Seq(
        (6L, " Testing a Dataframe With Five Words from with a set of stop words.")
      )).toDF("id", "lyrics")

      val tokens = WordTokenizer.tokenize(test_df, "lyrics" , "clean_tokens")
      val swremoved_length = SWRemover.removeStopWords(tokens).select("filtered lyrics").limit(1).first().getSeq(0).length
      swremoved_length
    }
  }


  "swremoval without Stop Words " should "have 7 tokens before and after removal. There should be no removal as there  are no SW. " in {
    assertResult(7) {
      lazy val spark: SparkSession = {
        SparkSession
          .builder()
          .master("local")
          .appName("spark test example")
          .getOrCreate()
      }

      val test_df = spark.createDataFrame(Seq(
        (6L, " Testing Non-SW Removed Dataframe With Five Words ")
      )).toDF("id", "lyrics")

      val tokens = WordTokenizer.tokenize(test_df, "lyrics" , "clean_tokens")
      val swremoved_length = SWRemover.removeStopWords(tokens).select("filtered lyrics").limit(1).first().getSeq(0).length
      swremoved_length
    }
  }


  "Utility flattenNestedTuple6 " should " test successfully " in{
    val testResult = (Try(1), Try("Stairway To Heaven"), Try(1971), Try("Led Zeplin") , Try("Rock"), Try("There's a lady who's sure" +
      "\nAll that glitters is gold" +
      "\nAnd she's buying a stairway to heaven" +
      "\nWhen she gets there she knows" +
      "\nIf the stores are all closed" +
      "\nWith a word she can get what she came for" +
      "\nOh oh oh oh and she's buying a stairway to heaven"), Try(10.0))
    assertResult(testResult) {
      lazy val spark: SparkSession = {
        SparkSession
          .builder()
          .master("local")
          .appName("spark test example")
          .getOrCreate()
      }

      val test_df = spark.createDataFrame(Seq(
        (6L, " Testing Non-SW Removed Dataframe With Five Words ")
      )).toDF("id", "lyrics")

      val test2 = ((Try(1), Try("Stairway To Heaven"), Try(1971), Try("Led Zeplin") , Try("Rock"),
        Try("There's a lady who's sure" +
          "\nAll that glitters is gold" +
          "\nAnd she's buying a stairway to heaven" +
          "\nWhen she gets there she knows" +
          "\nIf the stores are all closed" +
          "\nWith a word she can get what she came for" +
          "\nOh oh oh oh and she's buying a stairway to heaven")), Try(10.0))

      val flattenedList = Utility.flattenNestedTuple6(test2)
      flattenedList
    }
  }





  "CLean Data " should " test successfully " in{
    lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("spark test example")
        .getOrCreate()
    }

    val result_df = spark.createDataFrame(Seq(
      (6L, "There's a lady who's sure", "Theres a lady whos sure")
    )).toDF("id", "lyrics", "clean_lyrics")

    assertResult(result_df.rdd.first().getString(2)) {
      val test_df = spark.createDataFrame(Seq(
        (6L, "There's a lady who's sure")
      )).toDF("id", "lyrics")
      val cleanText = DataCleaner.cleanTrain(test_df)
      cleanText.rdd.first().getString(2)
    }
  }

}

