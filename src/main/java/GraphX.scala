import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphX {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkGraphX")
      .set("spark.executor.memory", "2g")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    //create a vertices RDD
    val vertices = Array((1L, "Kazan"), (2L, "Moscow"), (3L, "Petresburg"))
    val vericesRDD = sc.parallelize(vertices)

    //create edges RDD
    val edges = Array(Edge(1L, 2L, 810), Edge(1L, 3L, 1500), Edge(2L, 3L, 700));
    val edgesRDD = sc.parallelize(edges)

    //create a Graph
    val graph = Graph(vericesRDD, edgesRDD)

    //print all vertices of graph
    graph.vertices.foreach(println)
    //print all edges of graph
    graph.edges.foreach(println)

    //print edge triplets
    graph.triplets.collect.foreach(println)
  }
}
