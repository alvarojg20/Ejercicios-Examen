import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql._

object Exercise3 extends App {
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.option("header", "false").option("sep", "\t").csv("src/main/resources/retail_db/customers-tab-delimited")
  df.show()
  df.registerTempTable("customers")

  val results = df.sqlContext.sql("select distinct _c7 as State, count(_c7) as Count from customers group by _c7 having Count > 50")
  results.show()
}
