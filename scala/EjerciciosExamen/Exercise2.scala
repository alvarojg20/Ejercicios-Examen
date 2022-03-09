package EjerciciosExamen

import EjerciciosExamen.Spark
import org.apache.spark.sql.functions.{col, desc}

object Exercise2 extends App{
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.option("header", "false").csv("src/main/resources/EjerciciosExamen/retail_db/categories")
  df.show()

  val results = {
    df.select(col("_c0").cast("int"), col("_c2")).orderBy(desc("_c0"))
  }
  results.show()

  results.write.format("csv").mode("overwrite").option("header",true).option("sep",":").save("src/main/resources/EjerciciosExamen/exercise/solution2")
}
