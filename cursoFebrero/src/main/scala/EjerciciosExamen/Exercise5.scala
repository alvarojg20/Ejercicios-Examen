package EjerciciosExamen

import EjerciciosExamen.Spark
import org.apache.spark.sql.functions.{col, from_unixtime, lit, month, to_date, year}
import org.apache.spark.sql.types.{IntegerType, TimestampType}

object Exercise5 extends App {
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.option("header", "true").format("parquet").load("src/main/resources/EjerciciosExamen/retail_db/orders_parquet")
  df.show()

  val results = df.withColumn("newDate", to_date((col("order_date")/1000).cast(TimestampType))).select(col("order_id").cast(IntegerType)
    ,col("newDate")
    ,col("order_status"))
    .filter(col("order_status").equalTo("COMPLETE")
      && (month(col("newDate")).equalTo(7)
      || month(col("newDate")).equalTo(1))
      && year(col("newDate")).equalTo(2014))

  results.show()

  results.write.format("json").mode("overwrite").save("src/main/resources/EjerciciosExamen/exercise/solution5")
}
