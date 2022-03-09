package EjerciciosExamen

import org.apache.spark.sql.SparkSession

object Spark {
  def createLocalSession = {
    val sparkSession = SparkSession.builder().master(master = "local[*]").appName(name = "curso").config("spark.some.config.option", "some-value").getOrCreate()
    sparkSession
  }
}
