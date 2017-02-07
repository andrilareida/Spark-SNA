import org.scalatest.FunSuite


/**
  * Created by Andri on 06.02.2017.
  */
class ASNetWeightedTest extends FunSuite{

  test("An AS permutation should produce all edges and"){

    val size = 1 * 1024 * 1024 * 1024

    val iter = Iterable(ASrecord(1,20,size),
      ASrecord(2,5,size),
      ASrecord(3,5,size),
      ASrecord(4,10,size),
      ASrecord(5,1,size))
    val edges = ASNetWeighted.permutation(iter)

    assert(edges.length == iter.size*(iter.size-1)/2)
    val totalPeers = iter.map(_.peers).sum
    //Calculate the total downloads per as and subtract the part that is served internally. Divide by two
    val totalWeight=iter.map(x => x.size * x.peers - x.size * x.peers * x.peers/totalPeers).sum/(2 * math.pow(1024,3))

    edges.foreach(println(_))
    assert(edges.map(_.weight).sum.round == totalWeight.round)

  }


}
