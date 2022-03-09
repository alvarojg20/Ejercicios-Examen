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

  val mostCalls18 = df.select(col("CallDate"))
    .withColumn("CallDate",to_timestamp(col("CallDate"),"MM/dd/yy"))
    .groupBy(col("CallDate")).count()
    .filter(year(col("CallDate")).equalTo("2018"))
    .orderBy(desc("CallDate"))
  mostCalls18.show()

  val neighBour = df.select(col("Neighborhood"))
    .where(col("City").equalTo("SF"))
    .groupBy(col("Neighborhood")).count()
    .orderBy(desc("count"))
  neighBour.show()

  val delNeigh = df.select(col("Neighborhood"), col("Delay"))
    .orderBy(desc("Delay"))
  delNeigh.show()


  //Ejercicios IoT
  val dfIOT = sparkSession.read.option("header", "false").json("src/main/resources/EjerciciosSparkLearning/iot_devices.json")
  dfIOT.show()

  val umbral = dfIOT.filter(col("battery_level") < 5)
  umbral.show()

  val co2 = dfIOT.select("c02_level", "cn").orderBy(desc("c02_level"))
  co2.show()
}
