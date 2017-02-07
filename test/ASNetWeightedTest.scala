import org.scalatest.FunSuite


/**
  * Created by Andri on 06.02.2017.
  */
class ASNetWeightedTest extends FunSuite{

  test("An AS permutation should produce all edges and"){

    val size = 1 * 1024 * 1024

    val iter = Iterable(ASrecord(1,20,size),
      ASrecord(2,10,size),
      ASrecord(3,5,size))
    val edges = ASNetWeighted.permutation(iter)

    assert(edges.length == math.pow(iter.size,2))

    val totalWeight=iter.map(x => x.size *x.peers).sum/math.pow(1024,3)
    assert(edges.map(_.weight).sum == totalWeight)
    edges.foreach(println(_))
  }


}
