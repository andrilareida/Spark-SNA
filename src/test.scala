import java.util.{Calendar, Date, GregorianCalendar}

import org.apache.log4j.LogManager
import org.apache.log4j.nt.NTEventLogAppender
import org.apache.spark.sql.catalyst.expressions.Log
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Andri on 21.12.2016.
  */
object test {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val log = LogManager.getRootLogger

    val maxTorrents = 50
    val year = 2016
    val month = 5 to 7
    month.foreach(m => {
      val cal = new GregorianCalendar()
      cal.set(year, m-1, 1)
      val days = 1 to cal.getActualMaximum(Calendar.DAY_OF_MONTH)
      days.foreach(day => {
        val query = "FROM torrentsperip A, dailysharedtorrents B" +
          " SELECT A.infohash, A.peeruid " +
          " WHERE A.peeruid = B.peeruid" +
          " AND B.year = A.year = " + year +
          " AND B.month = A.month = " + m +
          " AND B.day = B.day = " + day +
          " AND B.shared BETWEEN 1 AND " + maxTorrents +
          " GROUP BY A.infohash, A.peeruid"
        log.info(query)
        val peertorrents = sqlContext.sql(query)

        peertorrents.map(pt => (pt(1), pt(0))).groupByKey()
          .flatMap{case (peer: String, hashes: Iterable[String]) =>
            permutation(hashes).map(edge => (edge, 1))}.reduceByKey(_ + _)
          .saveAsTextFile("/user/viola/torrentnet/"+m+"/"+day+"/")
      })
    })
    val day = 15




  }

  def permutation( iter:Iterable[String] ) : Array[String] ={
    val list = iter.toArray[String]
    var s = ArrayBuffer.empty[String]
    for (x <- 0 to (list.length - 2)) {
      val first = list(x)
      list.drop(x + 1).foreach(blah => {
        if (first.compareTo(blah) < 0) {
          //println(first + "\t" + blah)
          s+=(first + "\t" + blah)
        } else if (first.compareTo(blah) > 0) {
          // println(blah + "\t" + first)
          s+= (blah + "\t" + first)
        }
      })
    }
    s.toArray
  }

}
