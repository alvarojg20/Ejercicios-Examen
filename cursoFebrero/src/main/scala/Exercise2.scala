import org.apache.spark.sql.SparkSession

object Exercise2 extends App{
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.option("header", "false").csv("src/main/resources/retail_db/categories")
  df.show()
  df.registerTempTable("categorias")

  val results = df.sqlContext.sql("select _c0,_c2 from categorias order by CAST(_c0 as int) desc")
  results.show()
  //results.write.format("com.databricks.spark.csv").save("src/main/resources/retail_db/categories/ej2.csv")
}
