package bdm

import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

case class Income(
    _id: Int,
    neigh_name: String,
    district_id: Int,
    district_name: String,
    info: Array[IncomeInfo]
)
case class IncomeInfo(year: Int, pop: Int, RFD: Double)

case class Idealista(foo: Int)

case class SparkClient(spark: SparkSession) {
  import spark.implicits._

  var idealistaRDD: RDD[Idealista] = null
  var incomeRDD: RDD[Income] = null

  def loadAll(): Unit = {
    this.loadIncomeRDD()
  }

  private def loadIdealistaRDD(): Unit = {
    this.idealistaRDD = this.loadMongoRDD[Idealista]
  }

  private def loadIncomeRDD(): Unit = {
    this.incomeRDD = this.loadMongoRDD[Income]
  }

  private def loadMongoRDD[T: Encoder]: RDD[T] = {
    spark.sparkContext
      .loadFromMongoDB[Income](
        ReadConfig(
          Map(
            "uri" -> "mongodb://test:1234@127.0.0.1/test.income?authSource=admin"
          )
        )
      )
      .toDF()
      .as[T]
      .rdd
      .cache()
  }
}

object Main extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("myApp")
      .getOrCreate()

  val client : SparkClient = SparkClient(spark)

  try {
    client.loadAll()
  } catch {
    case e: Throwable =>
      println("===== ERROR =====")
      println(e.getMessage())
      println("===== ERROR =====")
      spark.close()
  }
}
