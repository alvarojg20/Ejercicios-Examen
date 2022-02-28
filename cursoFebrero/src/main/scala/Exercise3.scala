import org.apache.spark.sql.functions.{col, count}

object Exercise3 extends App {
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.option("header", "false").option("sep", "\t").csv("src/main/resources/retail_db/customers-tab-delimited")
  df.show()

  val results = df.filter(col("_c1").startsWith("A")).groupBy("_c7").count().filter(col("count") > 50).select(col("_c7").as("State"),col("count").as("Count"))
  results.show()

  results.write.format("parquet").option("compression","gzip").mode("overwrite").save("src/main/resources/exercise/solution3")
}
