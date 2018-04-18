package DataTransform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.regexp_replace

object DataCleaner {




  def cleanTrain(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("lyrics").isNotNull).withColumn("clean_lyrics", regexp_replace(df("lyricsWithGenre"), "[\\',\\n,\\t,\\r,\\r\\n,\\,]", ""))
    return dataframe
  }


  def cleanPredict(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("lyrics").isNotNull).withColumn("clean_lyrics", regexp_replace(df("lyrics"), "[\\',\\n,\\t,\\r,\\r\\n,\\,]", ""))
    return dataframe
  }


  def cleanGenre(df:DataFrame): DataFrame = {
    val dataframe = cleanGenreIndie(cleanGenreCountry(cleanGenreOther(cleanGenreRB(cleanGenreNA(cleanGenreFolk(cleanGenreJazz(
        cleanGenreMetal(cleanGenreHipHop(
        cleanGenreElectronic(cleanGenreRock(
        cleanGenrePop(df))))))))))))
    dataframe
  }

  def cleanGenrePop(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("clean_lyrics").isNotNull).withColumn("genre", regexp_replace(df("genre"), "Pop", "0.0"))
    return dataframe
  }

  def cleanGenreRock(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("clean_lyrics").isNotNull).withColumn("genre", regexp_replace(df("genre"), "Rock", "1.0"))
    return dataframe
  }

  def cleanGenreElectronic(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("clean_lyrics").isNotNull).withColumn("genre", regexp_replace(df("genre"), "Electronic", "2.0"))
    return dataframe
  }

  def cleanGenreHipHop(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("clean_lyrics").isNotNull).withColumn("genre", regexp_replace(df("genre"), "Hip-Hop", "3.0"))
    return dataframe
  }

  def cleanGenreMetal(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("clean_lyrics").isNotNull).withColumn("genre", regexp_replace(df("genre"), "Metal", "4.0"))
    return dataframe
  }

  def cleanGenreJazz(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("clean_lyrics").isNotNull).withColumn("genre", regexp_replace(df("genre"), "Jazz", "5.0"))
    return dataframe
  }


  def cleanGenreFolk(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("clean_lyrics").isNotNull).withColumn("genre", regexp_replace(df("genre"), "Folk", "6.0"))
    return dataframe
  }



  def cleanGenreNA(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("clean_lyrics").isNotNull).withColumn("genre", regexp_replace(df("genre"), "Not Available", "7.0"))
    return dataframe
  }



  def cleanGenreRB(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("clean_lyrics").isNotNull).withColumn("genre", regexp_replace(df("genre"), "R&B", "8.0"))
    return dataframe
  }



  def cleanGenreOther(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("clean_lyrics").isNotNull).withColumn("genre", regexp_replace(df("genre"), "Other", "9.0"))
    return dataframe
  }


  def cleanGenreCountry(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("clean_lyrics").isNotNull).withColumn("genre", regexp_replace(df("genre"), "Country", "10.0"))
    return dataframe
  }


  def cleanGenreIndie(df:DataFrame): DataFrame = {
    val dataframe = df.where(df("clean_lyrics").isNotNull).withColumn("genre", regexp_replace(df("genre"), "Indie", "11.0"))
    return dataframe
  }



}