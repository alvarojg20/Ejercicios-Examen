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
    .orderBy(desc("CallDate")).distinct()

  types18.show()

  val mostCalls18 = df.withColumn("CallDate",to_timestamp(col("CallDate"),"MM/dd/yy"))
    .withColumn("Mes", month(col("CallDate")))
    .filter(year(col("CallDate")).equalTo("2018"))
    .groupBy(col("Mes")).count()
    .select(col("Mes"), col("count").as("NumFireCalls"))
    .orderBy(desc("NumFireCalls"))

  mostCalls18.show()

  val neighBour = df.withColumn("CallDate",to_timestamp(col("CallDate"),"MM/dd/yy"))
    .filter(year(col("CallDate")).equalTo("2018"))
    .select(col("Neighborhood"))
    .where(col("City").equalTo("SF"))
    .groupBy(col("Neighborhood")).count()
    .orderBy(desc("count"))
  neighBour.show()

  val delNeigh = df.withColumn("CallDate",to_timestamp(col("CallDate"),"MM/dd/yy"))
    .filter(year(col("CallDate")).equalTo("2018"))
    .groupBy(col("Neighborhood")).avg("Delay")
    .orderBy(desc("avg(Delay)"))
  delNeigh.show()


  //Ejercicios IoT
  val dfIOT = sparkSession.read.option("header", "false").json("src/main/resources/EjerciciosSparkLearning/iot_devices.json")
  dfIOT.show()

  val umbral = dfIOT.filter(col("battery_level") < 5)
  umbral.show()

  val co2 = dfIOT.select("c02_level", "cn").orderBy(desc("c02_level"))
  co2.show()
}
