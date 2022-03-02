import org.apache.spark.sql.functions.{col, desc}

object ExerciseJoin1 extends App{
  implicit val sparkSession = Spark.createLocalSession

  val dfCustomers = sparkSession.read.option("header", "true").csv("src/main/resources/retail_db/customers")
  dfCustomers.show()

  val dfOrders = sparkSession.read.option("header", "false").csv("src/main/resources/retail_db/orders")
  dfOrders.show()

  val results = dfOrders.groupBy(col("_c2")).count().filter(col("count") > 5)
    .join(dfCustomers, col("_c2") === col("customer_id") , "inner" )
    .filter(col("customer_fname").startsWith("M"))
    .select("customer_fname","customer_lname", "count" )
    .orderBy(desc("count"))
  results.show()

  results.write.mode("overwrite").option("header","true").option("compression","gzip").option("sep","|").csv("src/main/resources/exercise/solutionJ1")
}
