package EjerciciosExamen

import EjerciciosExamen.Spark
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, concat_ws, lit, substring}

object Exercise6 extends App {
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.format("avro").load("src/main/resources/EjerciciosExamen/retail_db/customers-avro")
  df.show()

  val results = df.select(col("customer_id"),concat_ws(" ", col("customer_fname"), col("customer_lname")).as("customer_name"))
  results.show()

  results.write.format("csv").mode("overwrite").option("sep","\t").option("compression","bzip2").save("src/main/resources/EjerciciosExamen/exercise/solution6")
}
