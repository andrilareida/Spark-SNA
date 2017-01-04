import scala.collection.mutable.ArrayBuffer

/**
  * Created by Andri on 04.01.2017.
  * Produces a random
  */

case class Edge(from: String, to: String)

object Perm {
  def permutation(iter: Iterable[String]): Array[Edge] = {
    val list = iter.toArray[String]
    var s = ArrayBuffer.empty[Edge]
    for (x <- 0 to (list.length - 2)) {
      val first = list(x)
      list.drop(x + 1).foreach(blah => {
        if (first.compareTo(blah) < 0) {
          s += Edge(first, blah)
        } else if (first.compareTo(blah) > 0) {
          s += Edge(blah, first)
        }
      })
    }
    s.toArray
  }
}
