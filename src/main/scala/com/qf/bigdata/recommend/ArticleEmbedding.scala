package com.qf.bigdata.recommend

import com.qf.bigdata.recommend.ItemBaseFeature.log
import com.qf.bigdata.recommend.conf.Config
import com.qf.bigdata.recommend.hbase.HBaseUtil
import com.qf.bigdata.recommend.transformer.{ArticleEmbeddingModelData, UserBaseFeatureModelData}
import com.qf.bigdata.recommend.udfs.FeatureUDF
import com.qf.bigdata.recommend.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.udf
import org.slf4j.LoggerFactory

/**
  * @Description: 生产文章Embedding向量,写入到HBASE
  * @Author: QF    
  * @Date: 2020/7/28 4:39 PM   
  * @Version V1.0 
  */
object ArticleEmbedding {


  private val log = LoggerFactory.getLogger("article-embedding")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "ly")

    // 解析命令行参数
    val params = Config.parseConfig(ArticleEmbedding, args)
    log.warn("job running please wait ... ")
    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "article-embedding")
    // 基础数据处理
    val modelData = ArticleEmbeddingModelData(ss, params.env)
    // 读取用户特征数据
    val articleTermsVectorDF = modelData.loadArticleTermsVector()

    // 定义函数，将array[double] 转换为稠密向量, 因为我们之前存到表里的词向量是array[double]类型的
    // 这里转为稠密向量DenseVector，方便计算
    val array2vec = udf((array: Seq[Double]) => {
      Vectors.dense(array.toArray)
    })

    import ss.implicits._
    // 以文章ID为维度，将所有该文章下的所有词向量加和求平均得到文章向量
    val articleEmbeddingDF = articleTermsVectorDF
      .withColumn("vector", array2vec($"vector"))
      .groupBy("article_id")
      .agg(Summarizer.mean($"vector").alias("article_vector"))


    //  将features列SparseVector转换为string,保留将文章ID和其对应的特征向量两列
    val embeddingDF = articleEmbeddingDF
      .withColumn("article_vector", FeatureUDF.vector2Str($"article_vector"))
      .select("article_id", "article_vector")

    // 将特征向量保存一份到到HDFS
    embeddingDF.write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dwb_news.article_embedding")

    // 将特征向量存入到HBASE,先将其换换为HFile
    val hBaseUtil = HBaseUtil(ss, params.hbaseZK, params.hbaseZKPort)
    log.warn("start gen hfile, load article embedding  result to hbase ! ")
    // EmbeddingDataFrame 转换为HFile RDD
    val embeddingHFileRDD = modelData.articleEmbeddingDF2HFile(embeddingDF)
    // HFile RDD 生成文件后直接加载到HBASE中
    hBaseUtil.loadHfileRDD2Hbase(embeddingHFileRDD, params.tableName, params.hfileTmpPath)
    ss.stop()
    log.warn("job success! ")


  }
}