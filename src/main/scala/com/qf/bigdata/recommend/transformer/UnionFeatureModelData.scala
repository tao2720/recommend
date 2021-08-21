package com.qf.bigdata.recommend.transformer

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * @Description:
  * @Author: QF    
  * @Date: 2020/8/6 8:52 AM   
  * @Version V1.0 
  */
class UnionFeatureModelData(spark: SparkSession, env:String)  {


  /**
    * itemcf 结果和文章向量关联
    * @return
    */
  def genItemCFFeature():sql.DataFrame={

    val itemcfSql =
      """
         with t1 as(
          select * from dwb_news.itemcf
         ), t2 as (
         select t1.* ,abv.features from dwb_news.article_base_vector as abv inner join  t1
         on t1.sim_aid = abv.article_id
         ), t3 as (
         select  t2.* , article_vector from dwb_news.article_embedding  as ae inner join t2
         on t2.sim_aid = ae.article_id
         )
         select * from t3
      """.stripMargin

    val itemCFDF =  spark.sql(itemcfSql)
    itemCFDF
  }

  /**
    * als 结果和文章向量关联
    * @return
    */
  def genALSFeature():sql.DataFrame={

    val alsSql =
      """
          with t1 as(
          select * from dwb_news.als
          ), t2 as (
          select t1.* ,abv.features from dwb_news.article_base_vector as abv inner join  t1
          on t1.pred_aid = abv.article_id
          ), t3 as (
          select  t2.* ,article_vector from dwb_news.article_embedding  as ae inner join t2
          on t2.pred_aid = ae.article_id
          )
          select * from t3
      """.stripMargin

    val alsDF =  spark.sql(alsSql)
    alsDF
  }


  /**
    * als和itemcf两个结果的用户和用户稀疏向量关联
    * @return
    */
  def genUserFeature():sql.DataFrame={

    val userSql =
      """
          with t1 as(
          select uid from dwb_news.als
          union
          select  uid from dwb_news.itemcf
          ), t2 as (
          select t1.*, ubv.features from t1 inner join  dwb_news.user_base_vector as ubv
          on t1.uid = ubv.uid
          ) select uid,features as user_features from t2
      """.stripMargin

    val userDF =  spark.sql(userSql)
    userDF
  }





  def featureDataConvert(recoDF:DataFrame): sql.DataFrame={
    import spark.implicits._
    // (uid,sim_aid,pred_rating,features,article_vector) 转换为 [uid,(sim_aid,pred_rating,features,article_vector),...]
    recoDF.rdd
      .map(row => (row.getInt(0), (row.getInt(1), row.getDouble(2),row.getString(3),row.getString(4))))
      .groupByKey()
      .mapValues(sr => {
        var sequence = Seq[(Int , Double,String,String)]()
        sr.foreach(x=>{
          sequence :+= (x._1, x._2,x._3,x._4)
        })
        sequence.sortBy(-_._2)
      }).toDF("uid", "recommendations")
  }

  def featuresDF2HFile(featureDF:sql.DataFrame,column :String):RDD[(ImmutableBytesWritable,KeyValue)]={

    val hfileRdd = featureDF.rdd.sortBy(x =>x.get(0).toString).flatMap(row=>{
      val uid = row.getInt(0).toString
      val items = row.getAs[Seq[Row]](1).map(item=>item.getInt(0).toString+":"+item.getDouble(1).formatted("%.4f")+":"+
        item.getString(2)+":"+item.getString(3)).mkString(";")
      val listBuffer = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
      val kv1: KeyValue = new KeyValue(Bytes.toBytes(uid), Bytes.toBytes("f1"), Bytes.toBytes(column), Bytes.toBytes(items))
      // 多个列按列名字典顺序append
      listBuffer.append((new ImmutableBytesWritable, kv1))
      listBuffer
    })
    hfileRdd
  }



  def userFeaturesDF2HFile(userFeatureDF:sql.DataFrame,column :String):RDD[(ImmutableBytesWritable,KeyValue)]={

    val hfileRdd = userFeatureDF.rdd.sortBy(x =>x.get(0).toString).flatMap(row=>{
      val uid = row.getInt(0).toString
      val items = row.getString(1)
      val listBuffer = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
      val kv1: KeyValue = new KeyValue(Bytes.toBytes(uid), Bytes.toBytes("f1"), Bytes.toBytes(column), Bytes.toBytes(items))
      // 多个列按列名字典顺序append
      listBuffer.append((new ImmutableBytesWritable, kv1))
      listBuffer
    })
    hfileRdd
  }


}


object UnionFeatureModelData{
  def apply(spark: SparkSession, env: String): UnionFeatureModelData = new UnionFeatureModelData(spark, env)
}
