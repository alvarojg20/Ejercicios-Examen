object Exercise6 extends App {
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.format("avro").load("src/main/resources/retail_db/customers-avro")
  df.show()
  df.registerTempTable("customers")

  val results = df.sqlContext.sql("select customer_id, concat(left(customer_fname,1), ' ', customer_lname) as customer_name from customers")
  results.show()
}
