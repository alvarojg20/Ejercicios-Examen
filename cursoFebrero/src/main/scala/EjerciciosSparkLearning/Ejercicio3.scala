package EjerciciosSparkLearning

import EjerciciosExamen.Spark
import org.apache.spark.sql.functions.{col, substring, to_timestamp}
import org.apache.spark.sql.types.TimestampType

object Ejercicio3 extends App {
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.option("header", "true").csv("src/main/resources/EjerciciosSparkLearning/departuredelays.csv")
  df.show()

  //Create temporary view
  df.createOrReplaceTempView("us_delay_flights_tbl")

  //Do some querys over temporary view
  //Query1
  sparkSession.sql("""SELECT distance, origin, destination
     FROM us_delay_flights_tbl
     WHERE distance > 1000
     ORDER BY distance DESC""").show(10)

  //Query2
  sparkSession.sql("""SELECT date, delay, origin, destination
     FROM us_delay_flights_tbl
     WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
     ORDER by delay DESC""").show(10)

  //Query3
  sparkSession.sql("""SELECT delay, origin, destination,
     CASE
       WHEN delay > 360 THEN 'Very Long Delays'
       WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
       WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
       WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
       WHEN delay = 0 THEN 'No Delays'
       ELSE 'Early'
     END AS Flight_Delays
     FROM us_delay_flights_tbl
     ORDER BY origin, delay DESC""").show(10)
}
