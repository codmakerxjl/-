import com.mongodb.spark.MongoSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document
object SparkToNintendo {

  def main(args: Array[String]): Unit = {

    //连接mongodb数据库，并指定访问的数据集合和写入的数据集合
    val spark: SparkSession = SparkSession.builder().appName(getClass.getName).master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost/work1.test")
      .config("spark.mongodb.output.uri", "mongodb://localhost/dataset.Nintendo")
      .getOrCreate()
    //加载访问数据库中的数据集合的数据，并转换成dateframe形式
    import spark.implicits._
    val df: DataFrame = MongoSpark.load(spark)
    //实例化df,把它命名为vgsales,用于后续的集合名字
    df.createOrReplaceTempView("vgsales")

    //SQL语句进行数据查询
    val resDf: DataFrame = spark.sql(
      """
        |select Year ,count(*)
        |from vgsales
        |where Publisher = "Nintendo"
        |group by Year
        |order by Year DESC
      """.stripMargin)
    //通过rdd进行数据转换，将查询到的数据写会数据库
    val resultRDD: RDD[(String, String)] = resDf.as[(String,String)].rdd
    val docRDD: RDD[Document] = resultRDD.map(
      x => {
        val document = new Document()
        //设置数据属性
        document.append("Year", x._1).append("count", x._2)
        document
      }
    )
    MongoSpark.save(docRDD)

  }


}

