package EjerciciosSparkLearning

import EjerciciosExamen.Spark
import org.apache.spark.sql.functions.{col, desc, month, to_date, to_timestamp, year}
import org.apache.spark.sql.types.TimestampType

object Ejercicio2 extends App{
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/EjerciciosSparkLearning/sf-fire-calls.csv")
  df.show()

  val types18 = df.select(col("CallType"), col("CallDate"))
    .withColumn("CallDate",to_timestamp(col("CallDate"),"MM/dd/yy"))
    .filter(year(col("CallDate")).equalTo("2018"))
    .orderBy(desc("CallDate"))
  types18.show()


}
