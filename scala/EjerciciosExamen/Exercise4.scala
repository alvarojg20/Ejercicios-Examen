package EjerciciosExamen

import EjerciciosExamen.Spark
import org.apache.spark.sql.functions.col

object Exercise4 extends App{
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.option("header", "false").csv("src/main/resources/EjerciciosExamen/retail_db/categories")
  df.show()

  val results = df.filter(col("_c2") === "Soccer")
  results.show()

  results.write.format("com.databricks.spark.csv").mode("overwrite").save("src/main/resources/EjerciciosExamen/exercise/solution4")
}
