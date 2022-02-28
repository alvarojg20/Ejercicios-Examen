import org.apache.spark.sql.functions.col

object Exercise4 extends App{
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.option("header", "false").csv("src/main/resources/retail_db/categories")
  df.show()

  val results = df.filter(col("_c2") === "Soccer")
  results.show()

  results.write.format("com.databricks.spark.csv").mode("overwrite").save("src/main/resources/exercise/solution4")
}
