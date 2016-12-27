import org.apache.log4j.LogManager
import org.apache.log4j.nt.NTEventLogAppender
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Log
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Andri on 21.12.2016.
  */
object test {
  def main(args: Array[String]) {
    //System.setProperty("hadoop.home.dir", "C:\\Program Files\\Java")
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Torrent Net").setMaster("local"))

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //val textFile = sc.textFile("test.csv").map(line => line.split("\t"))

    val log = LogManager.getRootLogger
    val month = 5
    val day = 15
    val year = 2016
    val maxTorrents = 50
   val query = "FROM torrentsperip A, dailysharedtorrents B" +
      " SELECT A.infohash, A.peeruid " +
      " WHERE A.peeruid = B.peeruid" +
      " AND B.year = A.year = " + year +
      " AND B.month = A.month = " + month +
      " AND B.day = A.day = " + day +
      " AND B.hour = A.hour = 1" +
      " AND B.shared <= " + maxTorrents +
      " GROUP BY A.infohash, A.peeruid"
   log.info(query)
    val peertorrents = sqlContext.sql(query)

  // val peertorrents = sc.textFile("sparktestdata.csv").flatMap(line => line.split("\"))
   val blah = peertorrents.map(record => (record(1), record(0)))
    blah.collect().foreach(println)
    val group = blah.groupByKey()

    val edges=group.flatMap{case (peer: String, hashes: Iterable[String]) =>
        permutation(hashes).map(edge => (edge, 1))}.reduceByKey(_ + _)

      edges.saveAsTextFile("/user/viola/torrentnet/"+month+"/"+day+"/")
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

