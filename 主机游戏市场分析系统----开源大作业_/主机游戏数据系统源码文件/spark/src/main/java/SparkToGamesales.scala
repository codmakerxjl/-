import com.mongodb.spark.MongoSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document
object SparkToGamesales {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(getClass.getName).master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost/work1.test")
      .config("spark.mongodb.output.uri", "mongodb://localhost/dataset.GAMESALES")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = MongoSpark.load(spark)
    df.createOrReplaceTempView("vgsales")
    //SQL
    val resDf: DataFrame = spark.sql(
      """
        |select Publisher , sum(NA_Sales),sum(EU_Sales),sum(JP_Sales)
        |from vgsales
        |group by Publisher
        |order by sum(NA_Sales) DESC
        |limit 10
      """.stripMargin)

    val resultRDD: RDD[(String, String,String,String)] = resDf.as[(String,String,String,String)].rdd

    val docRDD: RDD[Document] = resultRDD.map(
      x => {
        val document = new Document()
        document.append("Publisher", x._1).append("NA_sales", x._2).append("EU_sales", x._3).append("JP_sales", x._4)
        document
      }
    )

    //写入mongo
    MongoSpark.save(docRDD)

  }


}

