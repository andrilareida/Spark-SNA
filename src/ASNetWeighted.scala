import java.util.{Calendar, GregorianCalendar}

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Andri Lareida on 04.01.2017.
  */

case class ASrecord(infohash : String, ASnumber: Int,  peers: Int, size: Double, unit: String)
case class DirectedWeightedEdge(from: String, to: String, weight: Double)
case class DirectedEdge(from: String, to: String)

object ASNetWeighted {

//Expected in array: 0=year, 1=month-from, 2=month-to, 3=maxTorrents, 4=delimiter, 5=outputBasePath
  def main(args: Array[String]) {
    if (args.length < 5) {
    println("Expected in array: 0=year, 1=month-from, 2=month-to, 3=maxTorrents, 4=delimiter, 5=outputBasePath")
    sys.exit(1)
    }

    val log = Logger.getLogger(getClass.getName)
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Country Net")
      .set("spark.executor.memory", "26g"))
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
        val query = "SELECT A.infohash, A.asnumber, count(distinct(A.peeruid)) as peers, C.torrent_size, C.size_unit " +
          "FROM torrentsperip as A JOIN dailysharedtorrents as B " +
          "ON ( A.peeruid = B.peeruid " +
          "AND B.year = A.year " +
          "AND B.month = A.month " +
          "AND B.day = A.day ) " +
          "JOIN torrents as C " +
          "ON (A.infohash = C.info_hash) " +
          "AND A.year = " + year + " " +
          "AND A.month = " + month + " " +
          "AND A.day = " + day + " " +
          "AND B.shared between 1 and " + maxTorrents + " " +
          "AND A.asnumber <> 0 " +
          "GROUP BY A.infohash, A.asnumber, C.torrent_size, C.size_unit"
        val pt = sqlContext.sql(query)
       /* val stage1 = pt.select(pt.col("infohash"), pt.col("asnumber"))
          .where(pt.col("asnumber").isNotNull
            .and(pt.col("asnumber").notEqual(0)))*/

        val stage2 = pt.map(
          record => (record.getString(0), ASrecord(record.getString(0),
            record.getInt(1),
            record.getInt(2),
            record.getDouble(3),
            record.getString(4))))
          .groupByKey()

        val stage3 = stage2.flatMap { case (infohash: String, records: Iterable[ASrecord]) =>
          permutation(records).map(edge => (DirectedEdge(edge.from, edge.to), edge.weight))
        }.reduceByKey(_ + _).map(edge => edge._1.from + delimiter + edge._1.to + delimiter + edge._2)
        stage3.saveAsTextFile(args(4) + "/maxtorrents" + maxTorrents + "/" + month + "/" + day)
      })


    })

  }

  def permutation(iter: Iterable[ASrecord]): Array[DirectedWeightedEdge] = {
    val totalPeers = iter.map(_.peers).sum
    var buf = new scala.collection.mutable.ArrayBuffer[DirectedWeightedEdge]()

    iter.foreach(record => {
      val size=record.peers * record.size * matchUnit(record.unit) / scala.math.pow(1024,3)
      buf=buf.++(iter.map(source => DirectedWeightedEdge(source.ASnumber.toString, record.ASnumber.toString, size * source.peers / totalPeers)))
    })
    buf.toArray
  }
  def matchUnit(unit: Any): Double = unit match {
    case "KB" => scala.math.pow(1024,1)
    case "MB" => scala.math.pow(1024,2)
    case "GB" => scala.math.pow(1024,3)
    case _ => 1
  }
}
