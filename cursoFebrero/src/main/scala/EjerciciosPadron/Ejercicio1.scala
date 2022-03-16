package EjerciciosPadron

import EjerciciosExamen.Spark
import org.apache.spark
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, lit, sum}

object Ejercicio1 extends App{
  implicit val sparkSession = Spark.createLocalSession

  val df = sparkSession.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv("src/main/resources/EjerciciosPadron/Rango_Edades_Seccion_202202.csv")
  df.show()

  //Enumera todos los barrios diferentes
  val barrios = df.select("DESC_BARRIO").distinct()
  barrios.show()

  //Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barrios
  //diferentes que hay.
  df.createTempView("Padron")
  sparkSession.sql("SELECT COUNT(DISTINCT DESC_BARRIO) FROM Padron").show()

  //Crea una nueva columna que muestre la longitud de los campos de la columna
  //DESC_DISTRITO y que se llame "longitud".
  val longitud = df.withColumn("Longitud", functions.length(col("DESC_DISTRITO")))
  longitud.show()

  //Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.
  val newCol5 = longitud.withColumn("Valor5", lit(5))
  newCol5.show()

  //Borra esta columna.
  val dropCol5 = newCol5.drop("Valor5")
  dropCol5.show()

  //Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO
  dropCol5.write.mode("overwrite").partitionBy("DESC_DISTRITO","DESC_BARRIO")
    .csv("src/main/resources/EjerciciosPadron/Particion")

  //Lanza una consulta contra el DF resultante en la que muestre el número total de
  //"espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres"
  //para cada barrio de cada distrito. Las columnas distrito y barrio deben ser las primeras en
  //aparecer en el show. Los resultados deben estar ordenados en orden de más a menos
  //según la columna "extranjerosmujeres" y desempatarán por la columna
  //"extranjeroshombres".
  val total = dropCol5.groupBy(col("DESC_DISTRITO"), col("DESC_BARRIO"))
    .agg(sum(col("EspanolesHombres")), sum(col("EspanolesMujeres")),
      sum(col("ExtranjerosHombres")),sum(col("ExtranjerosMujeres")))
    .select("DESC_DISTRITO","DESC_BARRIO", "EspanolesHombres", "EspanolesMujeres",
      "ExtranjerosHombres", "ExtranjerosMujeres").orderBy(col("ExtranjerosMujeres").desc(col("ExtranjerosHombres")))
  total.show()

  //Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con
  //DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres"
  //residentes en cada distrito de cada barrio. Únelo (con un join) con el DataFrame original a
  //través de las columnas en común.
  val totalEspHombres = dropCol5.groupBy(col("DESC_DISTRITO"),col("DESC_BARRIO")).sum("EspanolesHombres")
    .select(col("DESC_DISTRITO"), col("DESC_BARRIO"), col("sum(EspanolesHombres)"))

  val join = dropCol5.join(totalEspHombres, (col("totalEspHombres.DESC_DISTRITO") === dropCol5.col("DESC_DISTRITO"))
    && (col("totalEspHombres.DESC_BARRIO") === dropCol5.col("DESC_BARRIO")))

  join.show()
}
