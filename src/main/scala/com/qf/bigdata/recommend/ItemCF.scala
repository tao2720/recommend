package com.qf.bigdata.recommend

import com.qf.bigdata.recommend.conf.Config
import com.qf.bigdata.recommend.hbase.HBaseUtil
import com.qf.bigdata.recommend.transformer.ItemCFModelData
import com.qf.bigdata.recommend.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

/**
  * @Description: 基于物品的协同过滤, 使用余弦相似度，计算物品之间的相似度
  * @Author: QF    
  * @Date: 2020/7/16 6:22 PM   
  * @Version V1.0 
  */

object ItemCF {

  private val log = LoggerFactory.getLogger("item-cf")

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "ly")

    // 解析命令行参数
    val params = Config.parseConfig(ItemCF, args)
    log.warn("job running please wait ... ")
    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "item-cf")

    // 基础数据处理工具类
    val modelData = ItemCFModelData(ss, params.env)
    // 将用户原始行为数据转换为评分数据,
    val ratingDF= modelData.genUserRatingData()
    //将评分数据随机划分训练数据和测试数据
    val Array(training, test) = ratingDF.randomSplit(Array(0.8, 0.2))
    training.cache()
//    training.orderBy("uid")show(false)
    // 没有真实数据时，可以加载一些简单测试数据，运行算法流程
//  val Array(training, test)  = modelData.someTestData().randomSplit(Array(0.8,0.2))

    // 评分DataFrame 转换为分布式坐标矩阵

    val ratingMatrix = modelData.ratingDF2Matrix(training)

    // 分布式坐标矩阵转换为行索引矩阵，计算行索引矩阵各个列(aid)之间的相似度，得到相似度矩阵
    val similarityMartix=  ratingMatrix.toRowMatrix().columnSimilarities()

    // 相似度矩阵转换为DataFrame
    val simDF = modelData.similarityMatrix2DF(similarityMartix)

    simDF.sort("aid","sim_aid").show(100,false)

    // 评分DataFrame和相似度DataFrame关联，同时计算评分和相似度的乘积
    val joinDF = modelData.joinRatingAndSimilarity(training,simDF)
    joinDF.sort("uid","aid_x").show(100,false)
    joinDF.cache()
//    joinDF.show(false)
    val predictDF = modelData.predictForTestData(joinDF,test)
//    predictDF.show(false)
    // 评估器，这里用来计算预测出来的评分和原始测试集的评分的均方误差，来衡量训练出的模型优劣
    val evaluator = new RegressionEvaluator()
      .setLabelCol("rating")
      .setPredictionCol("pred_rating")
    // 计算均方误差
    val rmse = evaluator.setMetricName("rmse").evaluate(predictDF)
    log.warn(s"[ItemCF算法] 均方根误差(Root-mean-square error) = $rmse")

//    为所有用户推荐topK的内容
    val recoDF =  modelData.recommendForAllUser(joinDF,params.topK)
//    recoDF.show(false)
    // 推荐结果保存一份到到HDFS
    recoDF.write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dwb_news.itemcf")

    // 将推荐结果格式转换
   val convertDF = modelData.recommendDataConvert(recoDF)

    // 结果存储到HBASE中
    val hBaseUtil =  HBaseUtil(ss,params.hbaseZK,params.hbaseZKPort)
    log.warn("start gen hfile, load itemcf result to hbase ! ")
    // 将推荐结果的DataFrame转换为HFile RDD
    val hfileRDD = modelData.itemCFDF2HFile(convertDF)

    // HFile RDD 生成文件后直接加载到HBASE中
    hBaseUtil.loadHfileRDD2Hbase(hfileRDD,params.tableName,params.hfileTmpPath)
    ss.stop()
    log.warn("job success! ")

  }
}
