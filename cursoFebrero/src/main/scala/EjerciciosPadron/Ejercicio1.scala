package EjerciciosPadron

import EjerciciosExamen.Spark
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, sum}

object Ejercicio1 extends App{
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv("src/main/resources/EjerciciosPadron")
  df.show()

  //Calcular el total de EspanolesHombres, EspanolesMujeres, ExtranjerosHombres y
  //ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO para los distritos
  //CENTRO, LATINA, CHAMARTIN, TETUAN, VICALVARO y BARAJAS.
  val results = df.groupBy("DESC_DISTRITO", "DESC_BARRIO")
    .agg(sum("EspanolesHombres"), sum("EspanolesMujeres"), sum("ExtranjerosHombres"), sum("ExtranjerosMujeres"))



  results.show()
}
