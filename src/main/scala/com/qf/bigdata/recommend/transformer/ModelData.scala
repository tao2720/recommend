package com.qf.bigdata.recommend.transformer

import com.qf.bigdata.recommend.udfs.RatingUDF
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
  * @Description: 生成评分数据
  * @Author: QF    
  * @Date: 2020/7/21 10:47 AM   
  * @Version V1.0 
  */
class ModelData(spark: SparkSession, env:String) {


  spark.udf.register("action2rating",RatingUDF.action2rating _)

  var limitData = ""
  if (env.equalsIgnoreCase("dev")) {
    limitData = " limit 1000"
  }

  /**
    * 从数仓dwb_news.user_article_action读取原始用户文章行为数据, 数据格式如下
    * 51917125 | 9049382 | 点击   | 20200715
    * 51876174 | 9049074 | 点击   | 20200715
    * 51960095 | 9049573 | 收藏   | 20200715
    *
    * @return
    */
  def loadSourceUserArticleActionData(): sql.DataFrame = {

    val loadSourceSql =
      s"""
         |select uid,aid,action,action_date from dwb_news.user_article_action
      """.stripMargin

    spark.sql(loadSourceSql)
  }

  /**
    * 将用户对每个文章的行为转化为评分，评分的计算由action2rating udf完成
    */
  def genRatingToEachAction()={
    loadSourceUserArticleActionData()
      .createOrReplaceTempView("source_data")
    // user action to rating data
    val a2rSQL=
      """
        |select uid,aid,action,action_date,action2rating(action,action_date)as rating from source_data
      """.stripMargin
    spark.sql(a2rSQL).createOrReplaceTempView("source_data_rating")
  }

  /**
    * 一个用户对一个文章所有行为的评分求和，得到用户，文章，评分 数据表
    * +--------+-------+-------------------+
    * |uid     |aid    |rating             |
    * +--------+-------+-------------------+
    * |52041126|9048928|0.28577226400375366|
    *
    * @return
    */
  def genUserRatingData():sql.DataFrame={
    genRatingToEachAction()
    val ratingSQL=
      """
        |select cast(uid as bigint),cast(aid as bigint),cast(sum(rating) as double) as rating from source_data_rating group by uid,aid order by uid desc
      """.stripMargin
    spark.sql(ratingSQL)
  }



  /**
    * 从数仓dwb_news.article_base_info 读取抽取的文章基础信息数据， 数据格式如下
          article_id | article_num | img_num | type_name | pub_gap
      ------------+-------------+---------+-----------+---------
       73789      |        2178 |      18 | 娱乐      |       6
       73809      |         348 |       4 | 娱乐      |       5
       73355      |        1297 |       3 | 情感      |       5

    *
    * @return
    */
  def loadSourceArticleBaseInfoData(): sql.DataFrame = {

    val loadSourceSql =
      s"""
         select article_id,cast(article_num as int) ,cast(img_num as int),type_name,cast(pub_gap as int)
         from dwb_news.article_base_info
      """.stripMargin

    spark.sql(loadSourceSql)
  }

  /**
    * 生成简单测试数据，(用户,物品,评分), 当没有真实数据时，可以用如下数据作为算法输入做简单算法流程测试
    * @return
    */
  def someTestData():sql.DataFrame={
    val array = Seq(
      (3,107,5.0),
      (2,102,2.5),
      (5,106,4.0),
      (1,101,5.0),
      (1,102,3.0),
      (5,102,3.0),
      (3,101,2.5),
      (5,104,4.0),
      (1,103,2.5),
      (2,103,5.0),
      (5,105,3.5),
      (4,104,4.5),
      (5,103,2.0),
      (2,101,2.0),
      (4,101,5.0),
      (3,105,4.5),
      (4,103,3.0),
      (5,101,4.0),
      (4,106,4.0),
      (2,104,2.0),
      (3,104,4.0)).map(x=>(x._1.toLong,x._2.toLong,x._3.toDouble))

    spark.createDataFrame(array).toDF("uid","aid","rating")
  }
}
