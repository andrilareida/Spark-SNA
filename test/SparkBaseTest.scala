import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by Andri on 08.02.2017.
  */
class SparkBaseTest extends FunSuite with BeforeAndAfterAll{


  @transient private var _sc: SparkContext = _

  private val conf = new SparkConf().setAppName("Test").setMaster("local")

  def sc: SparkContext = _sc

  override def beforeAll(): Unit = {
    super.beforeAll()
    _sc = new SparkContext(conf)
    println("Setup done")

  }

  override def afterAll(): Unit = {
    super.afterAll()
    sc.stop()
  }
}
