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
   // val log = LogManager.getRootLogger
    val month = 5
    val day = 15
    val year = 2016
    val maxTorrents = 50
    val query = "FROM torrentsperip SELECT infohash, peeruid " +
      "WHERE year = " + year +
      " AND month = " + month +
      " AND day = " + day +
      " AND peeruid NOT EXISTS IN " +
      "(FROM dailysharedtorrents SELECT peeruid" +
      " WHERE year = " + year +
      " AND month = " + month +
      " AND day = " + day +
      " AND shared > " + maxTorrents + ")" +
      "GROUP BY infohash, peeruid"
   // log.info(query)
    val peertorrents = sqlContext.sql(query)


    peertorrents.map(pt => (pt(1), pt(0))).groupByKey()
      .flatMap{case (peer: String, hashes: Iterable[String]) =>
        permutation(hashes).map(edge => (edge, 1))}.reduceByKey(_ + _)
      .saveAsTextFile("/user/viola/torrentnet/"+month+"/"+day+"/")

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
