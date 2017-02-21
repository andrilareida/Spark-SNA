import scala.collection.mutable.ArrayBuffer

/**
  * Created by Andri on 04.01.2017.
  * Produces a random
  */

case class CustomEdge(from: String, to: String, weight: Int)

object Perm {
  def permutation(iter: Iterable[String]): Array[CustomEdge] = {
    val list = iter.toArray[String]
    var s = ArrayBuffer.empty[CustomEdge]
    for (x <- 0 to (list.length - 2)) {
      val first = list(x)
      list.drop(x + 1).foreach(blah => {
        if (first.compareTo(blah) < 0) {
          s += CustomEdge(first, blah, 1)
        } else if (first.compareTo(blah) > 0) {
          s += CustomEdge(blah, first, 1)
        }
      })
    }
    s.toArray
  }
}
