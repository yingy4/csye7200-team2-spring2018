package FeatureExtraction

import org.apache.spark.rdd.RDD
import Utility._
import scala.util.Try
import Stream._
import scala.collection.immutable.ListMap
import com.mashape.unirest.http.Unirest

object RhymeScheme {

  type inType = RDD[Tuple6[Try[Int], Try[String], Try[Int], Try[String], Try[String], Try[String]]]
  type outType = RDD[Try[String]]

  def transformWithRhymeScheme(inRDD : inType) : outType = {

    inRDD.map( rt => {

      val itlt = for ( t <- rt._6) yield {

        val listLines = t.replaceAll("[\"]","").trim.replaceAll("[ ]","+").split("\\n").toList

        def getSyllablesPerLine(acc : List[Try[Int]], line : String) : List[Try[Int]] = {

          val syllableTry = Try(Unirest.post(s"https://ipeirotis-readability-metrics.p.mashape.com/getReadabilityMetrics?text=$line").header("X-Mashape-Key", "2DakK9sBl0mshVU2bhO8CGRwQfHAp1G2xevjsnNaIX23Gjdiqc").header("Content-Type", "application/x-www-form-urlencoded").header("Accept", "application/json").asJson.getBody.getObject.get("SYLLABLES").asInstanceOf[Double].toInt)

          val newAcc = acc :+ syllableTry

          newAcc
        }

        val listSyllables = listLines.foldLeft(List[Try[Int]]())(getSyllablesPerLine);

        listSyllables

      }

      val slt = for (il <- convert(itlt)) yield {

        val min = il.min; val max = il.max; val t1 = min + ((max-min)/3); val t2 = min + ((max-min)*2/3);

        val sl = for (i <- il) yield

          if (i<t1) "s"

          else {

            if (i>t2) "l"

            else "m"

          }

        sl

      }

      val st = for (sl <- slt) yield {

        val txt = sl.mkString

        val N = txt.length

        val s1 = from (0) take (N/2)

        val occList = s1.foldLeft( ListMap[Int, String]() ) ((lm,i) => {

          var s2 = from (i+3)

          if ((i+6) > (N/2)) { s2 = s2 take (N/2 - i -3) } else { s2 = s2 take 6 }

          val lm2 = s2.foldLeft( ListMap[Int, String]() )((a,j) => {

            val subStr = txt.substring(i,j)
            val occ = searchSubstringOccurences(subStr, txt)

            a + (occ -> subStr)

          })

          lm2 ++ lm
        })

        val pattern = occList.max._2

        pattern

      }

      ( st )

    })

  }
}
