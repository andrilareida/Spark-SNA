import java.util.{Calendar, GregorianCalendar}

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Andri on 04.01.2017.
  */
object CountryNet {

//Expected in array: 0=year, 1=month-from, 2=month-to, 3=maxTorrents, 4=delimiter, 5=outputBasePath
  def main(args: Array[String]) {
    if (args.length < 5) {
    println("Expected in array: 0=year, 1=month-from, 2=month-to, 3=maxTorrents, 4=delimiter, 5=outputBasePath")
    sys.exit(1)
    }

    val log = Logger.getLogger(getClass.getName)
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Country Net")
      .set("spark.executor.memory", "16GB")
      .set("spark.yarn.executor.memoryOverhead", "4GB"))
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val delimiter = "\t"
    val maxTorrents = args(3).toInt
    val year = args(0).toInt
    val months = args(1).toInt to args(2).toInt
    log.info("Going through months: " + months.toString())
    months.foreach(month => {
      val cal = new GregorianCalendar()
      cal.set(year, month - 1, 1)
      val days = 1 to cal.getActualMaximum(Calendar.DAY_OF_MONTH)
      log.info("Going through days: " + days.toString())
      days.foreach(day => {
        log.info("Month: " + month + " Day: " + day)
        val query = "SELECT A.infohash, A.peeruid, A.country, A.asnumber " +
          "FROM torrentsperip as A JOIN dailysharedtorrents as B " +
          "ON ( A.peeruid = B.peeruid " +
          "AND B.year = A.year " +
          "AND B.month = A.month " +
          "AND B.day = A.day ) " +
          "AND A.year = " + year + " " +
          "AND A.month = " + month + " " +
          "AND A.day = " + day + " " +
          "AND B.shared between 1 and " + maxTorrents + " " +
          "GROUP BY A.infohash, A.peeruid, A.country, A.asnumber"
        val peertorrents = sqlContext.sql(query)
        val group = peertorrents.map(record => (record(0).toString,record(2).toString))
          .groupByKey()
        log.info("Output adter group:" + group.count())
        val edges = group.flatMap { case (infohash: String, countries: Iterable[String]) =>
          Perm.permutation(countries).map(edge => (edge, 1))
        }.reduceByKey(_ + _).map(edge => edge._1.from + delimiter + edge._1.to + delimiter + edge._2)
        log.info("Edges to write:" + edges.count())

        edges.saveAsTextFile(args(4) + "/maxtorrents" + maxTorrents + "/" + month + "/" + day)
      })
    })
  }
}
