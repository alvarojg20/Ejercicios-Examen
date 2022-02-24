import org.apache.spark.sql.SparkSession

object Main extends App {
  val valueForStringData = "data"
  println(valueForStringData)
  val resultado = Suma.suma(3, 5)
  println(resultado)

  implicit val sparkSession = Spark.createLocalSession

  sparkSession.sparkContext.setLogLevel("ERROR")
  val df = sparkSession.read.option("header", "true").csv("src/main/resources/retail_db/customers/part-m-00000 - Copy")
  df.show

}