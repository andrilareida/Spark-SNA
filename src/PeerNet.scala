import java.util.{Calendar, GregorianCalendar}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Andri on 04.01.2017.
  */
object PeerNet {

//Expected in array: 0=year, 1=month-from, 2=month-to, 3=maxTorrents, 4=delimiter, 5=outputBasePath
  def main(args: Array[String]) {
    if (args.length < 5) {
    println("Expected in array: 0=year, 1=month-from, 2=month-to, 3=maxTorrents, 4=delimiter, 5=outputBasePath")
    sys.exit(1)
    }
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val delimiter = "\t"
    val maxTorrents = args(3).toInt
    val year = args(0).toInt
    val months = args(1).toInt to args(2).toInt
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

        edges.map(edge => edge._1.from + delimiter + edge._1.to + delimiter + edge._2).saveAsTextFile(args(4) + "/maxTorrents" + maxTorrents + "/" + month + "/" + day + "/")
      })
    })
  }

}
