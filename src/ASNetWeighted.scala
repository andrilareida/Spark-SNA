import java.util.{Calendar, GregorianCalendar}

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Andri Lareida on 04.01.2017.
  */

case class ASrecord(ASnumber: Int, peers: Long, size: Double)

case class WeightedEdge(from: String, to: String, weight: Double)

case class DirectedEdge(from: String, to: String)

object ASNetWeighted {
  private val log = Logger.getLogger(getClass.getName)
  //Expected in array: 0=year, 1=month-from, 2=month-to, 3=maxTorrents, 4=delimiter, 5=outputBasePath
  def main(args: Array[String]) {
    if (args.length < 5) {
      println("Expected in array: 0=year, 1=month-from, 2=month-to, 3=maxTorrents, 4=delimiter, 5=outputBasePath")
      sys.exit(1)
    }
    val debug = if (args.length == 6) args(5).equals("debug") else false


    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Country Net")
      .set("spark.executor.memory", "26g")
      .set("spark.yarn.executor.memoryOverhead", "4096"))
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val delimiter = "\t"
    val maxTorrents = args(3).toInt
    val year = args(0).toInt
    val months = args(1).toInt to args(2).toInt
    val hours = 0 to 23
    log.info("Going through months: " + months.toString())
    months.foreach(month => {

      val cal = new GregorianCalendar()
      cal.set(year, month - 1, 1)
      val days = 1 to cal.getActualMaximum(Calendar.DAY_OF_MONTH)
      log.info("Going through days: " + days.toString())
      days.foreach(day => {
          log.info("Month: " + month + " Day: " + day)
          val result1 = stage1(sqlContext, year, month, day, maxTorrents)
          if (debug)
            result1.count()
          val result2 = stage2(result1)

          if (debug)
            result2.count()

          val result3 = stage3(result2)

          if (debug)
            result3.count()

          result3.map(edge => edge._1.from + delimiter + edge._1.to + delimiter + edge._2)
            .saveAsTextFile(args(4) + "/maxtorrents" + maxTorrents + "/" + month + "/" + day)
        })
      })

  }

  def stage1(sqc: SQLContext, year: Int, month: Int, day: Int,  maxTorrents: Int): DataFrame = {
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
    sqc.sql(query)
  }

  def stage2(stage1: DataFrame): RDD[(String, Iterable[ASrecord])] = {
    stage1.map(
      record => (record.getString(0), ASrecord(record.getInt(1),
        record.getLong(2),
        record.getFloat(3).toDouble * matchUnit(record.getString(4)))))
      .groupByKey()
  }

  def stage3(stage2: RDD[(String, Iterable[ASrecord])]): RDD[(DirectedEdge,Double)] = {
      stage2.context.union(
        stage2.values.collect().map(l =>
          combine(stage2.context.parallelize(l.toSeq))
        )).reduceByKey(_ + _)
  }

  def matchUnit(unit: Any): Double = unit match {
    case "KB" => scala.math.pow(1024, 1)
    case "MB" => scala.math.pow(1024, 2)
    case "GB" => scala.math.pow(1024, 3)
    case _ => 1
  }

  def combine(swarm: RDD[ASrecord]): RDD[(DirectedEdge, Double)] = {
    val totalPeers = swarm.map(_.peers).sum
    val result = swarm.cartesian(swarm).filter{ case (a: ASrecord,b: ASrecord)=> a.ASnumber < b.ASnumber}
      .map{case (a: ASrecord, b: ASrecord) =>
        (DirectedEdge(a.ASnumber.toString, b.ASnumber.toString),  getWeight(a,b,totalPeers))
      }
    result
  }

  def getWeight(a: ASrecord, b: ASrecord, totalPeers: Double): Double ={
    a.peers * b.peers * a.size /( scala.math.pow(1024, 3) * totalPeers)
  }
}
