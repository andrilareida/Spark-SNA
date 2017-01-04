import java.util.{Calendar, GregorianCalendar}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Andri on 21.12.2016.
  */
object torrentNet {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val delimiter = "\t"
    val maxTorrents = 100
    val year = 2016
    val months = 5 to 7
    months.foreach(month => {
      val cal = new GregorianCalendar()
      cal.set(year, month - 1, 1)
      val days = 1 to cal.getActualMaximum(Calendar.DAY_OF_MONTH)
      days.foreach(day => {
        val query = "SELECT A.infohash, A.peeruid " +
          "FROM torrentsperip as A JOIN dailysharedtorrents as B " +
          "ON ( A.peeruid = B.peeruid " +
          "AND B.year = A.year " +
          "AND B.month = A.month " +
          "AND B.day = A.day ) " +
          "AND A.year = " + year + " " +
          "AND A.month = " + month + " " +
          "AND A.day = " + day + " " +
          "AND B.shared between 1 and " + maxTorrents + " " +
          "GROUP BY A.infohash, A.peeruid"
        val peertorrents = sqlContext.sql(query)
        val group = peertorrents.select("infohash", "peeruid")
          .map(record => (record(1), record(0)))
          .groupByKey()

        val edges = group.flatMap { case (peer: String, hashes: Iterable[String]) =>
          Perm.permutation(hashes).map(edge => (edge, 1))
        }.reduceByKey(_ + _)

        edges.map(edge => edge._1.from + delimiter + edge._1.to + delimiter + edge._2).saveAsTextFile("/user/viola/torrentnet/maxtorrents" + maxTorrents + "/" + month + "/" + day + "/")
      })
    })
    }



    }
