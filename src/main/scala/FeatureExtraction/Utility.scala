package FeatureExtraction

import scala.util.Try

object Utility {

  def convert[X](xtlt : Try[List[Try[X]]]) : Try[List[X]] = {

    val xlt = for ( xtl <- xtlt)

      yield for ( xt <- xtl; if (xt.isSuccess) ) yield xt.get


    xlt

  }

  //ROBIN-KARP SUBSTRING SEARCH
  //val pat = "ssml"
  //val txt = "smlssmllmslmssmlslssmlssml"

  def searchSubstringOccurences(pat : String, txt : String) : Int = {

    val M = pat.length
    val N = txt.length
    val d = 3
    val q = 101
    import Stream._


    val h = (from (0) take (M-1)).foldLeft(1)((h,i) => (h*d)%q)

    val (p,t) = (from (0) take (M)).foldLeft((0,0))((a,i) => {
      val (p,t) = a
      ((d*p + pat.charAt(i))%q, (d*t + txt.charAt(i))%q)
    })

    val (_,_,occurences) = (from (0) take (N-M+1)).foldLeft((t,p,0))((a,i) => {

      val (t,p,o) = a

      def checkMatch : Int = if (t == p && txt.substring(i,i+M).equals(pat)) 1 else 0

      //println( (i+M)%N + " " + (((d*(t - txt.charAt(i)*h) + txt.charAt((i+M)%N))%q)+q) + " " + txt.substring(i,i+M))

      ( (((d*(t - txt.charAt(i)*h) + txt.charAt((i+M)%N))%q)+q)%q, p, o + checkMatch)

    })

    occurences

  }

  // flatten nested tuples 6
  type inType6 = Tuple2[Tuple6[Try[Int], Try[String], Try[Int], Try[String], Try[String], Try[String]], Try[Double]]
  type outType6 = Tuple7[Try[Int], Try[String], Try[Int], Try[String], Try[String], Try[String], Try[Double]]

  def flattenNestedTuple6(t : inType6) : outType6 = (t._1._1, t._1._2, t._1._3, t._1._4, t._1._5, t._1._6, t._2)

  // flatted nested tuples 7
  type inType7 = Tuple2[Tuple7[Try[Int], Try[String], Try[Int], Try[String], Try[String], Try[String], Try[Double]], Try[String]]
  type outType7 = Tuple8[Try[Int], Try[String], Try[Int], Try[String], Try[String], Try[String], Try[Double], Try[String]]

  def flattenNestedTuple7(t : inType7) : outType7 = (t._1._1, t._1._2, t._1._3, t._1._4, t._1._5, t._1._6, t._1._7, t._2)

}
