/**
  * Created by Andri on 07.02.2017.
  */
case class FakeHiveASObject(infohash: String, asnumber: Int, peers: Long, size: Float, unit: String)

object FakeHiveASObject {
  def fromCSV(line: String):FakeHiveASObject = {
    val f = line.split(",")
    FakeHiveASObject(f(0),f(1).toInt,f(2).toLong,f(3).toFloat,"GB")
  }
}
