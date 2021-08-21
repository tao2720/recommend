package com.qf.bigdata.recommend.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @constructor 根据环境变量参数创建Spark Session
  * @author QF
  * @date 2020/6/9 2:59 PM
  * @version V1.0
  */
object SparkHelper {


  def getSparkSession(env: String, appName: String):SparkSession = {

    env match {
      case "prod" => {
        val conf = new SparkConf()
          .setAppName(appName)
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          //          .set("spark.sql.hive.metastore.version","2.3.0")
          .set("spark.sql.cbo.enabled", "true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable", "true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
          .set("spark.debug.maxToStringFields","200")
          .set("spark.kryoserializer.buffer.max", "1024m")

        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()

      }

      case "dev" => {
        val conf = new SparkConf()
          .setAppName(appName + " DEV")
          .setMaster("local[6]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          //          .set("spark.sql.hive.metastore.version","1.2.1")
          .set("spark.sql.cbo.enabled", "true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable", "true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
          .set("spark.debug.maxToStringFields","200")
          .set("spark.kryoserializer.buffer.max", "1024m")

        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      }
      case _ => {
        println("not match env, exits")
        System.exit(-1)
        null

      }
    }
  }

}
