object Exercise5 extends App {
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.option("header", "true").format("parquet").load("src/main/resources/retail_db/orders_parquet")
  df.show()

  df.registerTempTable("orders")

  val results = df.sqlContext.sql("select order_id, order_date, order_status from orders where order_status = 'COMPLETE'")
  results.show()
}
