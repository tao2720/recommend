package com.qf.bigdata.recommend.hbase

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory
/**
  * @Description: 通过生成HFile的方式,将数据高效的写入HBASE
  * @Author: QF    
  * @Date: 2020/7/22 2:09 PM   
  * @Version V1.0 
  */
class HBaseUtil(spark: SparkSession,hbaseZK:String,hbaseZKPort:String) {

  private val log = LoggerFactory.getLogger("HBaseUtil")

  /**
    * 将hfile RDD加载到HBASE中
    * @param hfileRDD  hfile  RDD[(ImmutableBytesWritable,KeyValue)
    * @param tableName 加载到的hbase中的表名
    * @param hfileTmpPath hfile 临时存储目录
    */
  def loadHfileRDD2Hbase(hfileRDD:RDD[(ImmutableBytesWritable,KeyValue)],tableName: String,hfileTmpPath:String)={

    val hfilePath = hfileTmpPath +"/"+ String.valueOf(System.currentTimeMillis())
    val hbaseConf = setHBaseConfig()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val conn = ConnectionFactory.createConnection(hbaseConf)
    log.warn("create hbase connection: "+conn.toString)
    val admin = conn.getAdmin
    val table = conn.getTable(TableName.valueOf(tableName))
    val job = Job.getInstance(hbaseConf)
    //设置job的输出格式
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])
    HFileOutputFormat2.configureIncrementalLoad(job, table, conn.getRegionLocator(TableName.valueOf(tableName)))
    // 如果路径存在就删除，因为加了毫秒作为目录，因此一般不会存在
    deleteFileExist(hfilePath)

    hfileRDD.coalesce(10).saveAsNewAPIHadoopFile(hfilePath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    // load 正常执行完毕后， 在hdfs生成的hfile文件就被mv走，不存在了
    bulkLoader.doBulkLoad(new Path(hfilePath), admin, table, conn.getRegionLocator(TableName.valueOf(tableName)))
    log.warn("load hfile to hbase success! hfile path: "+hfilePath)

  }


  /**
    * 配置HBASE参数
    * @return
    */
  def setHBaseConfig():Configuration={
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, hbaseZK)
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, hbaseZKPort)
    hbaseConf
  }

  /**
    * 如果hdfs存在传入的文件路径，则删除
    * @param filePath  文件路径
    */
  def deleteFileExist(filePath: String): Unit ={
    val output = new Path(filePath)
    val hdfs = FileSystem.get(new URI(filePath), new Configuration)
    if (hdfs.exists(output)){
      hdfs.delete(output, true)
    }
  }
}

object HBaseUtil {
  def apply(spark: SparkSession,hbaseZK:String,hbaseZKPort:String): HBaseUtil = new HBaseUtil(spark,hbaseZK,hbaseZKPort)
}
