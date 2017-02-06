import org.scalatest.FunSuite


/**
  * Created by Andri on 06.02.2017.
  */
class ASNetWeightedTest extends FunSuite{

  test("An AS permutation should produce all edges and"){

    val size = 1
    val unit = "unit"
    val iter = Iterable(ASrecord("infoHash",1,20,size,unit),
      ASrecord("infoHash",2,10,size,unit),
      ASrecord("infoHash",3,5,size,unit))
    val edges = ASNetWeighted.permutation(iter)

    assert(edges.length == math.pow(iter.size,2))

    val totalWeight=iter.map(x => x.size *x.peers * ASNetWeighted.matchUnit(x.unit)).sum/math.pow(1024,3)
    assert(edges.map(_.weight).sum == totalWeight)
    edges.foreach(println(_))
  }


}
