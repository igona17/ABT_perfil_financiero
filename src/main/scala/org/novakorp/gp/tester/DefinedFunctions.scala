package org.novakorp.gp.tester

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, datediff, lit, when}

import scala.collection.mutable.ListBuffer

trait DefinedFunctions extends SparkSessionWrapper {

  def bcraAntiguedad(): DataFrame = {

    spark.udf.register("ANTIGUEDAD_SF", (x: Seq[String]) => {
      try {
        val f: String  = x.collectFirst{
          case i if i.toFloat == 0f => i
        }.getOrElse("-1")
        x.indexOf(f) + 1
      } catch {
        case _: Throwable => -1
      }
    })

    spark.sql(Queries.q_bcra_antiguedad)

  }

  def consumos_categorias_30d(consumosdf: DataFrame): DataFrame = {

    val operacionesArray = new ListBuffer[String]()

    for (i <- 1 to 16) {
      operacionesArray += "operaciones_categoria"+i+"_30d"
    }

    var consumo_30_cat = consumosdf.where(datediff(col("fecha_campania"),col("fecha_transaccion_new")) <= 30)

    consumo_30_cat = consumo_30_cat.groupBy(col("id_cliente_core"), col("id_categoria")).agg(count(col("mto_transaccion")).as("count_30"))

    consumo_30_cat = consumo_30_cat.groupBy(col("id_cliente_core"))
      .pivot("id_categoria")
      .sum("count_30").orderBy(col("id_cliente_core"))

    for (x <- consumo_30_cat.columns.filter(_ != "id_cliente_core")){
      consumo_30_cat = consumo_30_cat.withColumnRenamed(x, "operaciones_categoria"+x+"_30d")
    }

    for (operacion <- operacionesArray) {
      if (!consumo_30_cat.columns.toSeq.contains(operacion)) consumo_30_cat = consumo_30_cat.withColumn(operacion, lit(0))
    }

    // REORDER COLUMNS
    val clienteOperacionesArray = "id_cliente_core" +: operacionesArray
    val columns = clienteOperacionesArray.map(col)
    consumo_30_cat = consumo_30_cat.select(columns: _*)
    consumo_30_cat

  }

  def consumos_categorias_90d(consumosdf: DataFrame): DataFrame = {

    val operacionesArray = new ListBuffer[String]()

    for (i <- 1 to 16) {
      operacionesArray += "operaciones_categoria"+i+"_90d"
    }

    var consumo_90_cat = consumosdf.where(datediff(col("fecha_campania"),col("fecha_transaccion_new")) <= 90)

    consumo_90_cat = consumo_90_cat.groupBy(col("id_cliente_core"), col("id_categoria")).agg(count(col("mto_transaccion")).as("count_90"))

    consumo_90_cat = consumo_90_cat.groupBy(col("id_cliente_core"))
      .pivot("id_categoria")
      .sum("count_90").orderBy(col("id_cliente_core"))

    for (x <- consumo_90_cat.columns.filter(_ != "id_cliente_core")){
      consumo_90_cat = consumo_90_cat.withColumnRenamed(x, "operaciones_categoria"+x+"_90d")
    }

    for (operacion <- operacionesArray) {
      if (!consumo_90_cat.columns.toSeq.contains(operacion)) consumo_90_cat = consumo_90_cat.withColumn(operacion, lit(0))
    }

    // REORDER COLUMNS
    val clienteOperacionesArray = "id_cliente_core" +: operacionesArray
    val columns = clienteOperacionesArray.map(col)
    consumo_90_cat = consumo_90_cat.select(columns: _*)

    consumo_90_cat

  }

  def consumos_categorias_180d(consumosdf: DataFrame): DataFrame = {

    val operacionesArray = new ListBuffer[String]()

    for (i <- 1 to 16) {
      operacionesArray += "operaciones_categoria"+i+"_180d"
    }

    var consumo_180_cat = consumosdf.where(datediff(col("fecha_campania"),col("fecha_transaccion_new")) <= 180)

    consumo_180_cat = consumo_180_cat.groupBy(col("id_cliente_core"), col("id_categoria")).agg(count(col("mto_transaccion")).as("count_180"))

    consumo_180_cat = consumo_180_cat.groupBy(col("id_cliente_core"))
      .pivot("id_categoria")
      .sum("count_180").orderBy(col("id_cliente_core"))

    for (x <- consumo_180_cat.columns.filter(_ != "id_cliente_core")){
      consumo_180_cat = consumo_180_cat.withColumnRenamed(x, "operaciones_categoria"+x+"_180d")
    }

    for (operacion <- operacionesArray) {
      if (!consumo_180_cat.columns.toSeq.contains(operacion)) consumo_180_cat = consumo_180_cat.withColumn(operacion, lit(0))
    }

    // REORDER COLUMNS
    val clienteOperacionesArray = "id_cliente_core" +: operacionesArray
    val columns = clienteOperacionesArray.map(col)
    consumo_180_cat = consumo_180_cat.select(columns: _*)

    consumo_180_cat

  }

}
