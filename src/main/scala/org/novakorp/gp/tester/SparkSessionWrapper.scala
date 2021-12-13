package org.novakorp.gp.tester

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  val rootLogger: Logger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val sc: SparkConf =  new SparkConf()
    .setMaster("yarn")
    .set("spark.driver.extraLibraryPath","/etc/scala_libs/extra_jars/")
    .set("spark.sql.parquet.writeLegacyFormat","true")
    .set("spark.sql.autoBroadcastJoinThreshold","-1")
    .set("spark.hadoop.hive.exec.dynamic.partition.mode","nonstrict")
    .set("spark.hadoop.hive.exec.dynamic.partition","true")
    .set("hive.enforce.sorting", "false")
    .set("hive.enforce.bucketing", "false")
    .set("spark.executor.memory", "10g")
    .set("spark.executor.cores", "4")
    .set("spark.cores.max", "4")
    .set("spark.driver.memory","20g")
    .set("spark.sql.warehouse.dir", "hdfs://nameservice1/user/admin/dev")
    .set("spark.executor.extraClassPath","/opt/cloudera/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/lib/spark/jars/hive-exec-1.21.2.7.0.3.0-79.jar:/opt/cloudera/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/lib/spark/jars/parquet-hadoop-bundle-1.6.0.jar")
    .set("spark.sql.parquet.binaryAsString", "true")

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .appName("abt-bancos")
      .config(conf = sc)
      .master("yarn").getOrCreate()

}
