package com.qf.bigdata.recommend.transformer

import com.qf.bigdata.recommend.udfs.RatingUDF
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.explode

import scala.collection.mutable.ListBuffer

/**
  * @Description:
  * @Author: QF    
  * @Date: 2020/7/21 10:08 AM   
  * @Version V1.0 
  */
class ALSModelData(spark: SparkSession,env:String)  extends ModelData(spark: SparkSession,env:String) {

  import spark.implicits._

  /**
    * 从ALS推荐的结果中过滤掉用户已经产生过行为的物品
    * @param ratingDF 原始用户评分DF
    * @param alsDF als 给用户推荐的结果
    * @return
    */
  def filterALSRecommendForAllUser(ratingDF: sql.DataFrame,alsDF:sql.DataFrame):sql.DataFrame={

    // 将recommendations 数组转为多行
    val transDF =  alsDF.withColumn("recommendations",explode($"recommendations"))
      .withColumn("pred_aid",$"recommendations.aid")
      .withColumn("pred_rating",$"recommendations.rating")


    ratingDF.createOrReplaceTempView("user_rating")
    transDF.createOrReplaceTempView("als_pred")
    // 过滤掉用户已经有行为的物品
    val filterSQL=
      """
        |select cast(t1.uid as int) as uid,cast(t1.pred_aid as int) as pred_aid, cast(t1.pred_rating as double) as pred_rating
        |from als_pred as t1
        |left join user_rating as t2
        |on t1.pred_aid = t2.aid and t1.uid=t2.uid
        |where t2.rating is  null
      """.stripMargin
    val filterDF = spark.sql(filterSQL)
    filterDF

    // 转换成推荐列表格式
    // (uid,sim_aid,pred_rating) 转换为 [uid,(sim_aid,pred_rating),(sim_aid,pred_rating),...] 作为推荐结果 并根据预测评分排序
//    filterDF.rdd
//      .map(row => (row.getInt(0), (row.getInt(1), row.getDouble(2))))
//      .groupByKey()
//      .mapValues(sr => {
//        var sequence = Seq[(Int , Double)]()
//        sr.foreach(x=>{
//          sequence :+= (x._1, x._2)
//        })
//        sequence
//      }).toDF("uid", "recommendations")


  }
  /**
    * 推荐的结果按用户ID分组，合并为一列
    * (uid,sim_aid,pred_rating) 转换为 [uid,[(sim_aid,pred_rating),(sim_aid,pred_rating),...]] 作为推荐结果 并根据预测评分排序
    * @param recoDF
    * @return
    */
  def recommendDataConvert(recoDF:DataFrame): sql.DataFrame={
    import spark.implicits._
    // (uid,sim_aid,pred_rating) 转换为 [uid,[(sim_aid,pred_rating),(sim_aid,pred_rating),...]] 作为推荐结果 并根据预测评分排序
    recoDF.rdd
      .map(row => (row.getInt(0), (row.getInt(1), row.getDouble(2))))
      .groupByKey()
      .mapValues(sr => {
        var sequence = Seq[(Int , Double)]()
        sr.foreach(x=>{
          sequence :+= (x._1, x._2)
        })
        sequence.sortBy(-_._2)
      }).toDF("uid", "recommendations")
  }

  /**
    * 推荐算法ALS 产生的结果生成HFile RDD
    * 需要添加建立好hbase表，行键将会存入uid, 列族为 f1 , 列为 als ,值为推荐的物品和评分
    * 创建hbase表样例如：
    *   create_namespace 'recommend'
    *   create 'recommend:news-cf','f1'
    * @param alsDF als产生解推荐结果,格式 [uid,[[aid,rating],[aid,rating],...]]]
    * @return
    */
  def alsDF2HFile(alsDF:sql.DataFrame):RDD[(ImmutableBytesWritable,KeyValue)]={

    val hfileRdd = alsDF.rdd.sortBy(x =>x.get(0).toString).flatMap(row=>{
      val uid = row.getInt(0).toString
      val items = row.getAs[Seq[Row]](1).map(item=>item.getInt(0).toString+":"+item.getDouble(1).formatted("%.4f")).mkString(",")
      val listBuffer = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
      val kv1: KeyValue = new KeyValue(Bytes.toBytes(uid), Bytes.toBytes("f1"), Bytes.toBytes("als"), Bytes.toBytes(items))
      // 多个列按列名字典顺序append
      listBuffer.append((new ImmutableBytesWritable, kv1))
      listBuffer
    })
    hfileRdd
  }



}

object ALSModelData {
  def apply(spark: SparkSession, env: String): ALSModelData = new ALSModelData(spark, env)
}