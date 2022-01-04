import com.mongodb.spark.MongoSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

object SparkToPlatform {

  def main(args: Array[String]): Unit = {

    //连接mongodb数据库，并指定访问的数据集合和写入的数据集合
    val spark: SparkSession = SparkSession.builder().appName(getClass.getName).master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost/work1.test")
      .config("spark.mongodb.output.uri", "mongodb://localhost/dataset.GamePlatform")
      .getOrCreate()
    //加载访问数据库中的数据集合的数据，并转换成dateframe形式
    import spark.implicits._
    val df: DataFrame = MongoSpark.load(spark)
    //实例化df,把它命名为vgsales,用于后续的集合名字
    df.createOrReplaceTempView("vgsales")

    //SQL
    val resDf: DataFrame = spark.sql(
      """
        |select Platform ,count(*)
        |from vgsales
        |group by Platform
        |order by count(*) DESC
        |limit 10
      """.stripMargin)

    val resultRDD: RDD[(String, String)] = resDf.as[(String,String)].rdd

    //转化document对象
    val docRDD: RDD[Document] = resultRDD.map(
      x => {
        val document = new Document()
        document.append("Platform", x._1).append("sum", x._2)
        document
      }
    )

    //写入mongo
    MongoSpark.save(docRDD)

  }


}

