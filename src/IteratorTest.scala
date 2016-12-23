/**
  * Created by Andri on 22.12.2016.
  */
object IteratorTest {

  def main(args: Array[String]): Unit = {

    val list = Array("one", "two", "three")

    for (x <- 0 to (list.length - 2)) {
      val first = list(x)
      list.drop(x + 1).foreach(blah => {
        if (first.compareTo(blah) < 0) {
          println(first + "\t" + blah)
        } else if (first.compareTo(blah) > 0) {
          println(blah + "\t" + first)
        }
      })
    }
  }
}