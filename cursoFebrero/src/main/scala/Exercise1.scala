import org.apache.spark.sql.functions.col

object Exercise1 extends App {
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.format("avro").option("header", "true").load("src/main/resources/retail_db/products_avro")
  df.show()

  val results = df.filter(col("product_price") <= 23 && col("product_price") >= 20 && col("product_name").startsWith("Nike"))
  results.show()

  results.write.format("parquet").mode("overwrite").option("compression","gzip").save("src/main/resources/exercise/solution1/")
}
