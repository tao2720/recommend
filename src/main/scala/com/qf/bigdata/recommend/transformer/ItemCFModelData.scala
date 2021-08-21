package com.qf.bigdata.recommend.transformer

import com.qf.bigdata.recommend.udfs.RatingUDF
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * @Description: 为ItemCF模型训练提供数据处理方法
  * @Author: QF    
  * @Date: 2020/7/16 9:27 PM   
  * @Version V1.0 
  */
class ItemCFModelData(spark: SparkSession, env:String) extends ModelData(spark: SparkSession, env:String) {

  /**
    * 将用户评分表转换为分布式矩阵表示 输入source_data_rating 这个表
    * @param df
    * @return
    */
  def ratingDF2Matrix(df:sql.DataFrame): CoordinateMatrix={
    val matrixRDD = df.rdd.map{
      case Row(uid:Long, aid:Long, rating:Double )=>
        MatrixEntry(uid.toLong,aid.toLong,rating.toDouble)
    }
    val cm = new CoordinateMatrix(matrixRDD)
    cm
  }

  /**
    * 相似度矩阵转换为DataFrame
    * @param coordinateMatrix 由IndexedRowMatrix 的columnSimilarities，计算得到相似度坐标矩阵
    * @return
    */
  def similarityMatrix2DF(coordinateMatrix: CoordinateMatrix):sql.DataFrame={
    // 矩阵变换为String RDD
    val transformedRDD=coordinateMatrix.entries.map{
      case MatrixEntry(row: Long, col:Long, sim:Double) =>
        (row.toString,col.toString,sim)
    }
    // 经过columnSimilarity 计算得到的相似度矩阵对称矩阵，
    // 它只保留了是上三角矩阵，同时这个上三角矩阵去掉了物品A与其自身相似度(总是1)物品对和物品之间相似度是0的物品对
    // 我们矩阵转为DataFrame，将物品对补全
    val simDF =  spark.createDataFrame(transformedRDD).toDF("aid","sim_aid","sim")
    simDF.union(simDF.select("sim_aid","aid","sim"))
  }


  /**
    * 关联评分DF和物品相似度DF，并将用户对物品的评分点乘关联到物品的相似度，得到用户对单个物品相似物品的评分
    * 类似结果如下
    * +--------+-------+-------------------+-------+-------+------------------+-------------------+
    * |uid     |aid    |rating             |aid    |sim_aid|sim               |rsp = (rating * sim)             |
    * +--------+-------+-------------------+-------+-------+------------------+-------------------+
    * |51876453|9015927|0.09002494812011719|9015927|9049026|0.7453559924999298|0.06710063455582463|
    * |51814918|9049258|0.09002494812011719|9049258|9049597|0.9284766908852593|0.08358606592768356|
    * |51924704|9049258|0.22506237030029297|9049258|9049597|0.9284766908852593|0.20896516481920888|
    * |52015583|9048856|0.22506237030029297|9048856|9049649|1.0               |0.22506237030029297|
    * |51951576|9048965|0.22506237030029297|9048965|9048968|0.4472135954999579|0.10065095183373697|
    * +--------+-------+-------------------+-------+-------+------------------+-------------------+
    *
    * @param ratingDF
    * @param simDF
    * @return
    */
  def joinRatingAndSimilarity(ratingDF: sql.DataFrame,simDF:sql.DataFrame):sql.DataFrame={

    ratingDF.createOrReplaceTempView("user_rating")
    simDF.createOrReplaceTempView("sim_item")
    val joinSQL=
      """
        |select t1.uid, t1.aid,t1.rating,t2.aid as aid_x,t2.sim_aid,t2.sim, t1.rating*t2.sim as rsp
        |from user_rating as t1
        |left join sim_item as t2
        |on t1.aid = t2.aid
        |where t2.sim is not null
      """.stripMargin
    spark.sql(joinSQL)

  }


  /**
    * 为用户推荐top-k的内容，同时会过滤掉用户已经有过行为的内容
    * @param ratingSimDF 评分DF和相似DF JOIN
    * @param topK
    * @return
    */
  def recommendForAllUser(ratingSimDF :sql.DataFrame,topK:Int=25): sql.DataFrame={
    ratingSimDF.createOrReplaceTempView("rating_sim")

    val prefSQL =
      s"""
        |with t1 as
        |(
        |select uid,sim_aid,sum(rsp)/sum(sim) as pred_rating
        |from rating_sim
        |group by uid,sim_aid
        |), t2 as (
        |select t1.* from t1
        |left join
        |user_rating as ur
        |on t1.uid=ur.uid and t1.sim_aid=ur.aid
        |where ur.rating is null
        |),
        |t3 as (
        | select uid,sim_aid , pred_rating,
        | ROW_NUMBER() OVER (PARTITION BY uid order by pred_rating desc ) as num from t2
        |) select cast(uid as int ) as uid,cast(sim_aid as int) as sim_aid,cast(pred_rating as double) as pred_rating  from t3 where num <= $topK
        |
      """.stripMargin

    val prefDF =  spark.sql(prefSQL)
    prefDF
//    import spark.implicits._
//    // (uid,sim_aid,pred_rating) 转换为 [uid,[(sim_aid,pred_rating),(sim_aid,pred_rating),...]] 作为推荐结果 并根据预测评分排序
//    prefDF.rdd
//      .map(row => (row.getInt(0), (row.getInt(1), row.getDouble(2))))
//      .groupByKey()
//      .mapValues(sr => {
//        var sequence = Seq[(Int , Double)]()
//        sr.foreach(x=>{
//            sequence :+= (x._1, x._2)
//        })
//        sequence.sortBy(-_._2)
//      }).toDF("uid", "recommendations")
  }


  /**
    * 推荐的结果按用户ID分组，合并为一列
    * (uid,sim_aid,pred_rating) 转换为 [uid,[(sim_aid,pred_rating),(sim_aid,pred_rating),...]] 作为推荐结果 并根据预测评分排序
    * 最终返回DF结果类似如下
    * +-----+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    *  |uid  |recommended                                                                                                                                                                                                                                                                             |
    *  +-----+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    *  |51554|[[402, 0.3334009498357773], [200, 0.28577226400375366], [201, 0.28577226400375366], [195, 0.28577226400375366], [194, 0.28577226400375366], [202, 0.28577226400375366], [205, 0.2150895099015218], [262, 0.21475638339549794], [287, 0.20036066829231725], [281, 0.19467683094823043]]  |
    *  |51392|[[399, 0.28577226400375366], [402, 0.28577226400375366], [392, 0.2585274432595102], [385, 0.22768240869796444], [400, 0.22595028227749064], [342, 0.22110066923687446], [290, 0.21824826246055046], [292, 0.21410174015282366], [315, 0.2125652485650218], [289, 0.21254394031515117]]  |
    *  |51716|[[398, 0.3097893748819769], [241, 0.2795277971520104], [360, 0.26616003172023667], [293, 0.2640595963392586], [237, 0.26229175733792875], [382, 0.2595780625641913], [259, 0.25900229659766044], [280, 0.257957302723468], [233, 0.25741724160888974], [230, 0.25635324586719577]]      |
    *  |51717|[[374, 0.23492200852534456], [379, 0.2330988076172057], [348, 0.23255879571410426], [376, 0.23188501571896075], [354, 0.23092542423741497], [358, 0.22814669274760022], [364, 0.22771463292827812], [362, 0.2271160199054104], [368, 0.2245965690097818], [381, 0.22033059154721482]]   |
    *
    *
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
    * 推荐算法ItemCF 产生的结果生成HFile RDD
    * 需要添加建立好hbase表，行键将会存入uid, 列族为 f1 , 列为 itemcf ,值为推荐的物品和评分
    * 创建hbase表样例如：
    *    create_namespace 'recommend'
    *    create 'recommend:news-cf','f1'
    * @param itemCFDF itemCF 产生解推荐结果,格式 [uid,[[aid,rating],[aid,rating],...]]]
    * @return
    */
  def itemCFDF2HFile(itemCFDF:sql.DataFrame):RDD[(ImmutableBytesWritable,KeyValue)]={

    val hfileRdd = itemCFDF.rdd.sortBy(x =>x.get(0).toString).flatMap(row=>{
      val uid = row.getInt(0).toString
      val items = row.getAs[Seq[Row]](1).map(item=>item.getInt(0).toString+":"+item.getDouble(1).formatted("%.4f")).mkString(",")
      val listBuffer = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
      val kv1: KeyValue = new KeyValue(Bytes.toBytes(uid), Bytes.toBytes("f1"), Bytes.toBytes("itemcf"), Bytes.toBytes(items))
      // 多个列按列名字典顺序append
      listBuffer.append((new ImmutableBytesWritable, kv1))
      listBuffer
    })
    hfileRdd
  }

  def predictForTestData(ratingSimDF :sql.DataFrame,testDF:sql.DataFrame): sql.DataFrame={
    ratingSimDF.createOrReplaceTempView("rating_sim")
    testDF.createOrReplaceTempView("test_data")

    val predictSQL =
      s"""
         |with t1 as
         |(
         |select uid,sim_aid,sum(rsp)/sum(sim) as pred_rating
         |from rating_sim
         |group by uid,sim_aid
         |), t2 as (
         |select * from test_data
         |)
         |select t2.*,t1.pred_rating from t2
         |inner join t1
         |on t2.uid=t1.uid and t2.aid=t1.sim_aid
         |where t1.pred_rating is not null
      """.stripMargin

    val predictDF =  spark.sql(predictSQL)
    predictDF
  }


}

object ItemCFModelData {
  def apply(spark: SparkSession, env: String): ItemCFModelData = new ItemCFModelData(spark, env)
}