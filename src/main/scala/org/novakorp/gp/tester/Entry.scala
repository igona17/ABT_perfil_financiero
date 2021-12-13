package org.novakorp.gp.tester

import org.apache.spark.sql.functions.lit

object Entry extends SparkSessionWrapper with DefinedFunctions {

  def main(args: Array[String]): Unit = {
    val fecha_proceso: String = args(0)
    val generateDataBER = new GenerateABTDataBER()
    val dfBER = generateDataBER.generateDataFrame()
//    generateDataBER.generateCSV(dfBER,fecha_proceso)

    val generateDataBSF = new GenerateABTDataBSF()
    val dfBSF = generateDataBSF.generateDataFrame()
//    generateDataBSF.generateCSV(dfBSF,fecha_proceso)

    val final_df = dfBER.union(dfBSF)
    final_df.withColumn("fecha_proceso", lit(s"${fecha_proceso}")).write.mode("overwrite").insertInto("de_qua_3ref.abt_perfil_financiero")

  }

}
