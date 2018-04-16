package FeatureExtraction

import org.apache.spark.rdd.RDD
import com.mashape.unirest.http.HttpResponse
import com.mashape.unirest.http.JsonNode
import com.mashape.unirest.http.Unirest
import scala.util.Try

object ReadabilityScore {

  type inType = RDD[Tuple6[Try[Int], Try[String], Try[Int], Try[String], Try[String], Try[String]]]
  type outType = RDD[Tuple7[Try[Int], Try[String], Try[Int], Try[String], Try[String], Try[String], Try[Double]]]

  def transformWithScore(inRDD : inType) : outType = {

    val outRDD = inRDD.map( rt => {

      val srt = for ( t <- rt._6) yield {
        val lyricsFormatted = t.replaceAll("[\"]","")
                                .replaceAll("[\\n]",". ")
                                .trim
                                .replaceAll("[ ]","+")

        val readingScore = Try(Unirest.post(s"https://ipeirotis-readability-metrics.p.mashape.com/getReadabilityMetrics?text=$lyricsFormatted")
          .header("X-Mashape-Key", "2DakK9sBl0mshVU2bhO8CGRwQfHAp1G2xevjsnNaIX23Gjdiqc")
          .header("Content-Type", "application/x-www-form-urlencoded")
          .header("Accept", "application/json")
          .asJson
          .getBody
          .getObject
          .get("FLESCH_READING")
          .asInstanceOf[Double])

        readingScore

      }
      (rt._1, rt._2, rt._3, rt._4, rt._5, rt._6, srt)
      })
    }


}
