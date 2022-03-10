package EjerciciosSparkLearning

import EjerciciosExamen.Spark
import org.apache.spark.sql.functions.{col, desc, month, to_date, to_timestamp, year}
import org.apache.spark.sql.types.TimestampType

object Ejercicio2 extends App{
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/EjerciciosSparkLearning/sf-fire-calls.csv")
  df.show()

   //What were all the different types of fire calls in 2018?
  val types18 = df.select(col("CallType"), col("CallDate"))
    .withColumn("CallDate",to_timestamp(col("CallDate"),"MM/dd/yy"))
    .filter(year(col("CallDate")).equalTo("2018"))
    .orderBy(desc("CallDate")).distinct()

  types18.show()

  //What months within the year 2018 saw the highest number of fire calls?
  val mostCalls18 = df.withColumn("CallDate",to_timestamp(col("CallDate"),"MM/dd/yy"))
    .withColumn("Mes", month(col("CallDate")))
    .filter(year(col("CallDate")).equalTo("2018"))
    .groupBy(col("Mes")).count()
    .select(col("Mes"), col("count").as("NumFireCalls"))
    .orderBy(desc("NumFireCalls"))

  mostCalls18.show()

  //Which neighborhood in San Francisco generated the most fire calls in 2018?
  val neighBour = df.withColumn("CallDate",to_timestamp(col("CallDate"),"MM/dd/yy"))
    .filter(year(col("CallDate")).equalTo("2018"))
    .select(col("Neighborhood"))
    .where(col("City").equalTo("SF"))
    .groupBy(col("Neighborhood")).count()
    .orderBy(desc("count"))
  neighBour.show()

  //Which neighborhoods had the worst response times to fire calls in 2018?
  val delNeigh = df.withColumn("CallDate",to_timestamp(col("CallDate"),"MM/dd/yy"))
    .filter(year(col("CallDate")).equalTo("2018"))
    .groupBy(col("Neighborhood")).avg("Delay")
    .orderBy(desc("avg(Delay)"))
  delNeigh.show()


  //Ejercicios IoT
  val dfIOT = sparkSession.read.option("header", "false").json("src/main/resources/EjerciciosSparkLearning/iot_devices.json")
  dfIOT.show()

  //Detect failing devices with battery levels below a threshold.
  val umbral = dfIOT.filter(col("battery_level") < 5).orderBy(desc("battery_level")).select("battery_level", "device_name")
  umbral.show()

  //Identify offending countries with high levels of CO2 emissions.
  val co2 = dfIOT.select("c02_level", "cn").orderBy(desc("c02_level")).distinct().limit(5)
  co2.show()

  //Compute the min and max values for temperature, battery level, CO2, and humidity.
  //Sort and group by average temperature, CO2, humidity, and country.
}
