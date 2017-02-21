import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Andri on 16.02.2017.
  */
class ASGraphX {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      System.err.println(
        "Should be one parameter: <path/to/edges>")
      System.exit(1)
    }
    val sc = new SparkContext(new SparkConf().setAppName("GraphX"))



    val edges: RDD[Edge[Double]] =
      sc.textFile(args(0)).map { line =>
        val fields = line.split("\t")
        Edge(fields(0).toLong, fields(1).toLong, fields(2).toDouble)
      }

    val graph: Graph[Int, Double] = Graph.fromEdges(edges,1)

  }

}
