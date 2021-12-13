package org.novakorp.gp.tester

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, to_date}

class GenerateABTDataBSF extends SparkSessionWrapper with DefinedFunctions {

  def generateDataFrame(): DataFrame = {

    val fecha_campania_df = spark.sql(Queries.fecha_campania)
    val fecha_campania = fecha_campania_df.first().get(0).toString

    var audiencias_clientes_df = spark.sql(Queries.q_audiencias_clientes_bsf)
    audiencias_clientes_df = audiencias_clientes_df.where(audiencias_clientes_df("fecha_resultado") === fecha_campania)
      .orderBy("id_cliente_core")

    val bcraRiesgo_df = spark.sql(Queries.q_bcra_riesgo)

    val bcraAntiguedad_df = bcraAntiguedad

    val consumos_df = spark.sql(Queries.consumos_bsf)
      .withColumn("fecha_transaccion_new", to_date(col("fecha_transaccion"),"yyyyMMdd"))
      .withColumn("fecha_campania", to_date(lit(fecha_campania),"yyyyMMdd"))

    val consumos_categorias_30d_df = consumos_categorias_30d(consumos_df)
    val consumos_categorias_90d_df = consumos_categorias_90d(consumos_df)
    val consumos_categorias_180d_df = consumos_categorias_180d(consumos_df)

    val final_df = audiencias_clientes_df
      .join(bcraRiesgo_df
        ,audiencias_clientes_df.col("id_persona") === bcraRiesgo_df.col("id_persona")
        ,"left")
      .drop(bcraRiesgo_df.col("id_persona"))
      .join(bcraAntiguedad_df
        ,audiencias_clientes_df.col("id_persona") === bcraAntiguedad_df.col("id_persona")
        ,"left")
      .drop(bcraAntiguedad_df.col("id_persona"))
      .join(consumos_categorias_30d_df
        ,audiencias_clientes_df.col("id_cliente_core") === consumos_categorias_30d_df.col("id_cliente_core")
        ,"left")
      .drop(consumos_categorias_30d_df.col("id_cliente_core"))
      .join(consumos_categorias_90d_df
        ,audiencias_clientes_df.col("id_cliente_core") === consumos_categorias_90d_df.col("id_cliente_core")
        ,"left")
      .drop(consumos_categorias_90d_df.col("id_cliente_core"))
      .join(consumos_categorias_180d_df
        ,audiencias_clientes_df.col("id_cliente_core") === consumos_categorias_180d_df.col("id_cliente_core")
        ,"left")
      .drop(consumos_categorias_180d_df.col("id_cliente_core"))
      .select(
      audiencias_clientes_df.col("sexo"),
      audiencias_clientes_df.col("estado_civil"),
      audiencias_clientes_df.col("generacion"),
      audiencias_clientes_df.col("monto_presunto"),
      audiencias_clientes_df.col("producto_ofrecido"),
      audiencias_clientes_df.col("resultado_campania"),
      audiencias_clientes_df.col("producto_adquirido"),
      audiencias_clientes_df.col("fecha_adquisicion"),
      audiencias_clientes_df.col("canal_venta"),
      audiencias_clientes_df.col("id_campania_sms"),
      audiencias_clientes_df.col("id_campania_cc"),
      audiencias_clientes_df.col("fecha_resultado"),
      audiencias_clientes_df.col("id_prospecto"),
      audiencias_clientes_df.col("entidad"),
      bcraRiesgo_df.col("riesgo_crediticio_1m"),
      bcraRiesgo_df.col("riesgo_crediticio_3m"),
      bcraRiesgo_df.col("riesgo_crediticio_6m"),
      bcraAntiguedad_df.col("antiguedad_sistema_financiero"),
      consumos_categorias_30d_df.col("operaciones_categoria1_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria2_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria3_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria4_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria5_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria6_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria7_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria8_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria9_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria10_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria11_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria12_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria13_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria14_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria15_30d"),
      consumos_categorias_30d_df.col("operaciones_categoria16_30d"),
      consumos_categorias_90d_df.col("operaciones_categoria1_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria2_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria3_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria4_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria5_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria6_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria7_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria8_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria9_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria10_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria11_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria12_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria13_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria14_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria15_90d"),
      consumos_categorias_90d_df.col("operaciones_categoria16_90d"),
      consumos_categorias_180d_df.col("operaciones_categoria1_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria2_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria3_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria4_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria5_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria6_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria7_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria8_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria9_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria10_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria11_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria12_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria13_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria14_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria15_180d"),
      consumos_categorias_180d_df.col("operaciones_categoria16_180d")
    )

    final_df

  }

  def generateCSV(df: DataFrame,fecha_proceso:String): Unit = {

    df.repartition(1)
      .write
      .mode("overwrite")
      .option("delimiter","|")
      .option("header","true")
      .csv(s"hdfs://nameservice1/user/admin/dev/test/03-ref/abt_csv/${fecha_proceso}/abt_ber")

  }
}
