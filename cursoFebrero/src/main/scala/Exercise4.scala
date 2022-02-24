object Exercise4 extends App{
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.option("header", "false").csv("src/main/resources/retail_db/categories")
  df.show()
  df.registerTempTable("categorias")

  val results = df.sqlContext.sql("select _c0, _c2 from categorias where _c2 = 'Soccer'")
  results.show()
}
