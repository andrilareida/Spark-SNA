import java.util.{Calendar, GregorianCalendar}
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
    val maxTorrents = 100
    val year = 2016
    val months = 5 to 7
    months.foreach(f = month => {
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
          permutation(hashes).map(edge => (edge, 1))
        }.reduceByKey(_ + _)

        edges.saveAsTextFile("/user/viola/torrentnet/maxtorrents" + maxTorrents + "/" + month + "/" + day + "/")
      })
    })
    }

      def permutation(iter: Iterable[String]): Array[String] = {
        val list = iter.toArray[String]
        var s = ArrayBuffer.empty[String]
        for (x <- 0 to (list.length - 2)) {
          val first = list(x)
          list.drop(x + 1).foreach(blah => {
            if (first.compareTo(blah) < 0) {
              //println(first + "\t" + blah)
              s += (first + "\t" + blah)
            } else if (first.compareTo(blah) > 0) {
              // println(blah + "\t" + first)
              s += (blah + "\t" + first)
            }
          })
        }
        s.toArray
      }

    }
