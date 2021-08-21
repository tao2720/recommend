package com.qf.bigdata.recommend.transformer

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * @Description: 为文章向量生产提供数据转换方法
  * @Author: QF    
  * @Date: 2020/7/28 4:41 PM   
  * @Version V1.0 
  */
class ArticleEmbeddingModelData(spark: SparkSession, env:String) extends ModelData(spark: SparkSession, env:String) {


  /**
    * 从数仓dwb_news.article_top_terms_w2v 表中读取文章关键词向量数据
    *
    * article_id | top_term | vector
    * ------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------
    * 8862607    | 减速慢行 | [0.03496983274817467, 0.17339648306369781, -0.11213770508766174, -0.02299109846353531, 0.09417295455932617, -0.013225400820374489, 0.200927
    * 8862607    | 弱者     | [-0.08087116479873657, -0.047462671995162964, 0.026697132736444473, -0.030793823301792145, 0.06060231477022171, 0.056847646832466125, -0.07
    * 8862606    | 设计师   | [-0.22160547971725464, -0.02786044403910637, -0.14247123897075653, -0.15862725675106049, -0.27565187215805054, -0.02081381343305111, 0.1681
    * 8862606    | 美感     | [-0.1846056580543518, -0.2657729983329773, -0.09711218625307083, -0.27734848856925964, -0.5568212270736694, 0.06630297005176544, 0.01408153
    *
    * @return
    */
  def loadArticleTermsVector(): sql.DataFrame = {

    val loadSourceSql =
      s"""
         |select  article_id,top_term,vector from dwb_news.article_top_terms_w2v where vector is not null
      """.stripMargin

    spark.sql(loadSourceSql)
  }


  /**
    * ArticleEmbedding 产生的结果生成HFile RDD
    * 需要添加建立好hbase表，行键将会存入uid, 列族为 f1 , 列为 embedding ,值为物品特征的向量
    * 创建hbase表样例如：
    *   create_namespace 'recommend'
    *   create 'recommend:news-feature','f1'
    * @param articleEmbeddingDF  生成物品特征ItemBaseFeature向量，格式[article_id,SparseVector.toString]
    * @return
    */
  def articleEmbeddingDF2HFile(articleEmbeddingDF:sql.DataFrame):RDD[(ImmutableBytesWritable,KeyValue)]={

    val hfileRdd = articleEmbeddingDF.rdd.sortBy(x =>x.get(0).toString).flatMap(row=>{
      val itemId = row.getString(0)
      val features = row.getString(1)
      val listBuffer = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
      val kv1: KeyValue = new KeyValue(Bytes.toBytes(itemId), Bytes.toBytes("f1"), Bytes.toBytes("embedding"), Bytes.toBytes(features))
      // 多个列按列名字典顺序append
      listBuffer.append((new ImmutableBytesWritable, kv1))
      listBuffer
    })
    hfileRdd
  }


}

object ArticleEmbeddingModelData {
  def apply(spark: SparkSession, env: String): ArticleEmbeddingModelData = new ArticleEmbeddingModelData(spark, env)
}