package bdm

import org.apache.spark.sql.SparkSession
import scala.math.random

object Main extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("myApp")
      .getOrCreate()
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt
    val count = spark.sparkContext.parallelize(1 until n, slices).map { _ =>
      val x = random() * 2 - 1
      val y = random() * 2 - 1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)

    println(s"Pi is roughly ${4.0 * count / (n - 1)}")

    spark.stop()
}
