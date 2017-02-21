import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Andri Lareida on 04.01.2017.
  */

case class ASrecordRatio(ASnumber: Int, peers: Long, size: Double, seeders: Int, leechers: Int)
object ASNetWeightedDriver {
  private val log = Logger.getLogger(getClass.getName)
  val delimiter = "\t"
  var year: Int= _
  var month: Int = _
  var day: Int =  _
  //Expected in array: 0=year, 1=month-from, 2=month-to, 3=maxTorrents, 4=delimiter, 5=outputBasePath
  def main(args: Array[String]) {
    if (args.length < 5) {
      println("Expected in array: 0=year, 1=month-from, 2=month-to, 3=maxTorrents, 4=delimiter, 5=outputBasePath")
      sys.exit(1)
    }
    year = args(0).toInt
    month = args(1).toInt
    day=  args(2).toInt
    val maxTorrents = args(3).toInt
    val hours: Range = if(args(5).equals("hourly")) 0 to 23 else toInt(args(5)) to toInt(args(5))

    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("AS Net Weighted " + maxTorrents
      + ' ' + year + '-' + month + '-' +day))
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    log.info("Month: " + month + " Day: " + day)
    for(hour<-hours) {
      val result1 = if (hour < 0) stage1(sqlContext, year, month, day, maxTorrents) else stage1(sqlContext, year, month, day, hour, maxTorrents)
      val query = "SELECT infohash, sum(seeder)as seeders, sum(leecher) as leechers " +
        "FROM intervaltrackerstats " +
        "WHERE total<>0 " +
        "AND year = " + year + " " +
        "AND month = " + month + " " +
        "AND day = " + day + " " +
        "GROUP BY infohash"

      val result2 = stage2(result1, sqlContext.sql(query))

      val result3 = stage3(result2, sqlContext.sql(query))

      val path = if (hour < 0) args(4) + "/maxtorrents" + maxTorrents + "/" + month + "/" + day
      else args(4) + "/maxtorrents" + maxTorrents + "/" + month + "/" + day + "/" + hour

      result3.map(edge => edge._1.from + delimiter + edge._1.to + delimiter + edge._2)
        .saveAsTextFile(path)
    }
  }

  def stage2(stage1: DataFrame, ratio: DataFrame): RDD[(String, Iterable[ASrecordRatio])] = {
    stage1.join(ratio, stage1("infohash") === ratio("infohash")).select().map(
      record => (record.getString(0), ASrecordRatio(record.getInt(1),
        record.getLong(2),
        record.getFloat(3).toDouble * matchUnit(record.getString(4)), record.getInt(6), record.getInt(7))))
      .groupByKey()
  }

  def stage3(stage2: RDD[(String, Iterable[ASrecordRatio])], trackerStats: DataFrame): RDD[(DirectedEdge,Double)] = {
    stage2.values.flatMap(combine).reduceByKey(_+_)
  }

  def matchUnit(unit: Any): Double = unit match {
    case "KB" => scala.math.pow(1024, 1)
    case "MB" => scala.math.pow(1024, 2)
    case "GB" => scala.math.pow(1024, 3)
    case _ => 1
  }

  def combine(swarm: Iterable[ASrecordRatio]): Iterable[(DirectedEdge, Double)] = {
    val totalPeers = swarm.map(_.peers).sum
    for (a <- swarm; b <- swarm if a.ASnumber < b.ASnumber ) yield {
      (DirectedEdge(a.ASnumber.toString, b.ASnumber.toString),  getWeight(a,b,totalPeers))
    }
  }

  def getWeight(a: ASrecordRatio, b: ASrecordRatio, totalPeers: Double): Double ={

    if(a.seeders == 0){
      0
    }else {
      (a.peers * b.peers * a.size * a.leechers.toDouble) / (scala.math.pow(1024, 3) * totalPeers * (a.seeders+a.leechers))
    }
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

  def stage1(sqc: SQLContext, year: Int, month: Int, day: Int, hour: Int, maxTorrents: Int): DataFrame = {
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
      "AND A.hour = " + hour + " " +
      "AND B.shared between 1 and " + maxTorrents + " " +
      "AND A.asnumber <> 0 " +
      "GROUP BY A.infohash, A.asnumber, C.torrent_size, C.size_unit"
    sqc.sql(query)
  }

  def toInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => -1
    }
  }
}
