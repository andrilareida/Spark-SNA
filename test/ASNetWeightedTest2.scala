/**
  * Created by Andri on 06.02.2017.
  */
class ASNetWeightedTest2 extends SparkBaseTest{

  test("An AS permutation should produce all edges and"){

    val size = 1 * 1024 * 1024 * 1024
    val seeder = 2
    val leecher = 10
    val seq = Seq(ASrecordRatio(1,20,size,seeder,leecher),
      ASrecordRatio(2,5,size,seeder,leecher),
      ASrecordRatio(3,5,size,seeder,leecher),
      ASrecordRatio(4,10,size,seeder,leecher),
      ASrecordRatio(5,1,size,seeder,leecher))
    val edges = ASNetWeightedDriver.combine(seq)

    assert(edges.size == seq.size*(seq.size-1)/2)
    val totalPeers = seq.map(_.peers).sum
    //Calculate the total downloads per as and subtract the part that is served internally. Divide by two
    val totalWeight=seq.map(x => x.size * x.peers - x.size * x.peers * x.peers/totalPeers).sum/(2 * math.pow(1024,3))

    edges.foreach(println(_))
    assert(edges.map(p => p._2).sum.round == (totalWeight* leecher/(seeder+leecher)).round)

  }

  test("Stage 2 should produce a correct mapping of torrents to AS records"){
    //val conf = new SparkConf().setAppName("Test").setMaster("local")
   // val context = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val input = sqlContext.createDataFrame(Seq(
      "infohash1,1,20,3","infohash1,2,10,3","infohash1,3,15,3"
    ).map(FakeHiveASObject.fromCSV))

    val stats = sqlContext.createDataFrame(
      Seq("infohash1,200,300","infohash2,100,100","infohash9,100,50")
        .map(FakeRatioObject.fromCSV))

    val res = ASNetWeightedDriver.stage2(input, stats)
    assert(res.keys.count() == 1)
    assert(res.values.map(_.size).sum() == input.count())
  }

  test("stage3 must connect all the ASes which share a torrent"){
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val input = sqlContext.createDataFrame(Seq(
      "infohash1,1,10,1","infohash1,2,10,1","infohash1,3,10,1",
      "infohash2,1,10,1","infohash2,5,10,1",
      "infohash9,3,10,1"
    ).map(FakeHiveASObject.fromCSV))
    val stats = sqlContext.createDataFrame(
      Seq("infohash1,200,300","infohash2,100,100","infohash9,100,50")
        .map(FakeRatioObject.fromCSV))

    val res = ASNetWeightedDriver.stage2(input, stats)


    val edges = ASNetWeightedDriver.stage3(res, stats)

    assert(edges.count() == 4)

  }


}
