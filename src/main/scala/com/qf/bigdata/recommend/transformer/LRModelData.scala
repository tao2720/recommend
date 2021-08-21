package com.qf.bigdata.recommend.transformer

import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegressionModel}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
  * @Description: 逻辑回归模型数据处理
  * @Author: QF    
  * @Date: 2020/7/29 5:06 PM   
  * @Version V1.0 
  */
class LRModelData(spark: SparkSession, env: String) extends ModelData(spark: SparkSession, env: String) {

  def getVectorTrainingData(): sql.DataFrame = {
    val vectorSql =
      s"""
          with t1 as (
          select  uid,aid,label from dwb_news.user_item_training
          ) ,t2 as (
          select t1.*,ubv.features as user_features from t1 left join dwb_news.user_base_vector as ubv
          on t1.uid = ubv.uid
          where ubv.uid is not null and ubv.features <> ''
          ) ,t3 as (
          select t2.*, abv.features as article_features from t2 left join dwb_news.article_base_vector as abv
          on t2.aid = abv.article_id
          where abv.article_id is not null and abv.features <> ''
          ), t4 as (
          select t3.*, ae.article_vector as article_embedding from t3 left join dwb_news.article_embedding as ae
          on t3.aid = ae.article_id
          where ae.article_id is not null and ae.article_vector <> ''
          )
          select uid as uid,aid as aid,user_features as user_features,article_features as article_features, article_embedding as article_embedding, cast(label as int) as label from t4
      """.stripMargin

    spark.sql(vectorSql)
  }

  def printSummary(lrModel: LogisticRegressionModel): Unit = {
    val trainingSummary = lrModel.summary
    // 获取每个迭代目标函数的值
    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.zipWithIndex.foreach{case (loss,iter) =>{
      println(s"iterator: ${iter}, loss: ${loss}")
//      println(loss)
    }}

    println(s"accuracy:" +trainingSummary.accuracy)
    // 获取模型评估指标来衡量模型的表现
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
    // 获取roc DataFrame
//     val roc = binarySummary.roc
//     roc.show()
    // 打印模型的AUC
    println(s"auc: ${binarySummary.areaUnderROC}")

  }


}


object LRModelData {
  def apply(spark: SparkSession, env: String): LRModelData = new LRModelData(spark, env)
}