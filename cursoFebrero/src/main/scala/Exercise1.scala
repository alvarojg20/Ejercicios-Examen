import Main.sparkSession
import org.apache.spark.sql.{SQLContext, SparkSession}

object Exercise1 extends App {
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.format("avro").option("header", "true").load("src/main/resources/retail_db/products_avro/part-m-00000.avro")
  df.show()
  df.registerTempTable("products")

  val results = df.sqlContext.sql("select * from products where product_price >= 20 and product_price <=23 and product_name like 'Nike%'")
  results.show()
  //results.write.format("com.databricks.spark.csv").save("ej2.csv")
}
