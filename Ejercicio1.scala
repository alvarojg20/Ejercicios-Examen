package EjerciciosSparkLearning

import EjerciciosExamen.Spark
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{avg, col, count, desc}

object Ejercicio1 extends App {
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.text("src/main/resources/EjerciciosSparkLearning/el_quijote.txt")
  df.show()
  df.count()
  df.head(5)
  df.first()
  df.take(3)


  val mnmDF = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/EjerciciosSparkLearning/mnm_dataset.csv")
  // Aggregate counts of all colors and groupBy() State and Color
  // orderBy() in descending order
  val countMnMDF = mnmDF.select("State", "Color", "Count").groupBy("State", "Color").agg(count("Count").alias("Total")).orderBy(desc("Total"))
  // Show the resulting aggregations for all the states and colors
  countMnMDF.show()
  // Find the aggregate counts for California by filtering
  val caCountMnMDF = mnmDF.select("State", "Color", "Count").where(col("State") === "CA" || col("State") === "NV" || col("State") === "CO" || col("State") === "TX").groupBy("State", "Color").agg(count("Count").alias("Total")).orderBy(desc("Total"))
  // Show the resulting aggregations for California
  caCountMnMDF.show()

  val functDF = mnmDF.groupBy(col("State"),col("Color"))
    .agg(functions.min("Count").as("Minimo"),avg("Count").as("Average"),functions.max("Count").as("Maximo"))
    .orderBy("State")
  functDF.show()

}
