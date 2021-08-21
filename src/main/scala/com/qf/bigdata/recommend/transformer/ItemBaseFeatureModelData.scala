package com.qf.bigdata.recommend.transformer

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * @Description: 为物品基础特征转换提供数据处理方法
  * @Author: QF    
  * @Date: 2020/7/24 5:27 PM   
  * @Version V1.0 
  */
class ItemBaseFeatureModelData(spark: SparkSession, env:String) extends ModelData(spark: SparkSession, env:String) {


  /**
    * ItemBaseFeature 产生的结果生成HFile RDD
    * 需要添加建立好hbase表，行键将会存入uid, 列族为 f1 , 列为 base ,值为物品特征的向量
    * 创建hbase表样例如：
    *   create_namespace 'recommend'
    *   create 'recommend:news-feature','f1'
    * @param itemBaseFeatureDF  生成物品特征ItemBaseFeature向量，格式[article_id,SparseVector.toString]
    * @return
    */
  def itemBaseFeatureDF2HFile(itemBaseFeatureDF:sql.DataFrame):RDD[(ImmutableBytesWritable,KeyValue)]={

    val hfileRdd = itemBaseFeatureDF.rdd.sortBy(x =>x.get(0).toString).flatMap(row=>{
      val itemId = row.getString(0)
      val features = row.getString(1)
      val listBuffer = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
      val kv1: KeyValue = new KeyValue(Bytes.toBytes(itemId), Bytes.toBytes("f1"), Bytes.toBytes("base"), Bytes.toBytes(features))
      // 多个列按列名字典顺序append
      listBuffer.append((new ImmutableBytesWritable, kv1))
      listBuffer
    })
    hfileRdd
  }



}


object ItemBaseFeatureModelData{
  def apply(spark: SparkSession, env: String): ItemBaseFeatureModelData = new ItemBaseFeatureModelData(spark, env)
}