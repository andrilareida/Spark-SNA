/**
  * Created by Andri on 21.02.2017.
  */
case class FakeRatioObject(info_hash: String, seeders: Long, leechers: Long)

object FakeRatioObject {
  def fromCSV(line: String): FakeRatioObject = {
    val f = line.split(",")
    FakeRatioObject(f(0), f(1).toInt, f(2).toInt)
  }
}
