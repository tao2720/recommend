package com.qf.bigdata.recommend

import com.qf.bigdata.recommend.conf.Config
import com.qf.bigdata.recommend.hbase.HBaseUtil
import com.qf.bigdata.recommend.transformer.UserBaseFeatureModelData
import com.qf.bigdata.recommend.udfs.FeatureUDF
import com.qf.bigdata.recommend.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.udf
import org.slf4j.LoggerFactory

/**
  * @Description: 生成用户基本特征向量，并存储到HBASE中
  * @Author: QF    
  * @Date: 2020/7/28 12:37 PM   
  * @Version V1.0 
  */
object UserBaseFeature {

  private val log = LoggerFactory.getLogger("user-base-feature")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "root")

    // 解析命令行参数
    val params = Config.parseConfig(UserBaseFeature, args)
    log.warn("job running please wait ... ")
    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "user-base-feature")
    // 基础数据处理
    val modelData = UserBaseFeatureModelData(ss, params.env)
    // 读取用户特征数据
    val userFeatureDF = modelData.loadSourceUserBaseFeature()

    // 为gender列值创建索引表示
    val genderIndexer = new StringIndexer()
      .setInputCol("gender").setOutputCol("gender_index")
    // 为email列创建索引表示
    val emailIndexer = new StringIndexer()
      .setInputCol("email_suffix").setOutputCol("email_index")

    // 对gender email 特征进行one-hot 编码
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array(genderIndexer.getOutputCol, emailIndexer.getOutputCol))
      .setOutputCols(Array("gender_vec", "email_vec"))

    // 对于age特征我们划分年龄段为 [0,15,25,35,50,60] 表示 0-15, 15-25 依次类推
    // Bucketizer 就可以帮我们做这件事
    val splits = Array(0, 15, 25, 35, 50, 60, Double.PositiveInfinity)
    val bucketizer = new Bucketizer()
      .setInputCol("age")
      .setOutputCol("age_buc")
      .setSplits(splits)

    // 对bucket 后的 age 列进行最小最大归一化
    val ageAssembler = new VectorAssembler()
      .setInputCols(Array("age_buc"))
      .setOutputCol("age_vec")
    val featuresScaler = new MinMaxScaler()
      .setInputCol("age_vec")
      .setOutputCol("age_scaler")

    // 合并归一化后的数值特征列[article_num,img_num,pub_gap]和one-hot后的type_name列合并为一个向量
    val assembler = new VectorAssembler()
      .setInputCols(Array("gender_vec", "email_vec", "age_scaler"))
      .setOutputCol("features")

    // 定义一个Pipeline,将各个特征转换操作放入到其中处理
    val pipeline = new Pipeline()
      .setStages(Array(genderIndexer, emailIndexer, encoder, bucketizer, ageAssembler, featuresScaler, assembler))

    // 将数据集itemFeatureDF，就是我们的文章内容信息数据作用到我们定义的pipeline上
    val pipelineModel = pipeline.fit(userFeatureDF)
    // 对我们的数据集执行定义的转换操作
    val featuresDF = pipelineModel.transform(userFeatureDF)
    featuresDF.show(false)
    import ss.implicits._

    /*featuresDF
+------+------+---+------------+------------+-----------+-------------+---------------+-------+-------+----------+----------------------------+
|uid   |gender|age|email_suffix|gender_index|email_index|gender_vec   |email_vec      |age_buc|age_vec|age_scaler|features                    |
+------+------+---+------------+------------+-----------+-------------+---------------+-------+-------+----------+----------------------------+
|110129|男    |27 |qq.com      |1.0         |14.0       |(1,[],[])    |(17,[14],[1.0])|2.0    |[2.0]  |[0.4]     |(19,[15,18],[1.0,0.4])      |
|110128|女    |48 |139.net     |0.0         |10.0       |(1,[0],[1.0])|(17,[10],[1.0])|3.0    |[3.0]  |[0.6]     |(19,[0,11,18],[1.0,1.0,0.6])|
|110125|男    |37 |msn.com     |1.0         |16.0       |(1,[],[])    |(17,[16],[1.0])|3.0    |[3.0]  |[0.6]     |(19,[17,18],[1.0,0.6])      |
|110127|女    |30 |qq.com      |0.0         |14.0       |(1,[0],[1.0])|(17,[14],[1.0])|2.0    |[2.0]  |[0.4]     |(19,[0,15,18],[1.0,1.0,0.4])|
|110126|女    |78 |github.com  |0.0         |13.0       |(1,[0],[1.0])|(17,[13],[1.0])|5.0    |[5.0]  |[1.0]     |(19,[0,14,18],[1.0,1.0,1.0])|
|110130|男    |0  |yahoo.com   |1.0         |6.0        |(1,[],[])    |(17,[6],[1.0]) |0.0    |[0.0]  |[0.0]     |(19,[7],[1.0])              |
|110132|女    |16 |sdu.edu.cn  |0.0         |12.0       |(1,[0],[1.0])|(17,[12],[1.0])|1.0    |[1.0]  |[0.2]     |(19,[0,13,18],[1.0,1.0,0.2])|
|110131|男    |45 |qq.com      |1.0         |14.0       |(1,[],[])    |(17,[14],[1.0])|3.0    |[3.0]  |[0.6]     |(19,[15,18],[1.0,0.6])      |
|110303|男    |54 |123.com     |1.0         |17.0       |(1,[],[])    |(17,[],[])     |4.0    |[4.0]  |[0.8]     |(19,[18],[0.8])             |
|110304|男    |55 |126.cn      |1.0         |2.0        |(1,[],[])    |(17,[2],[1.0]) |4.0    |[4.0]  |[0.8]     |(19,[3,18],[1.0,0.8])       |
|110301|男    |30 |123.com     |1.0         |17.0       |(1,[],[])    |(17,[],[])     |2.0    |[2.0]  |[0.4]     |(19,[18],[0.4])             |
|110302|男    |9  |msn.com     |1.0         |16.0       |(1,[],[])    |(17,[16],[1.0])|0.0    |[0.0]  |[0.0]     |(19,[17],[1.0])             |
|110288|女    |12 |123.com     |0.0         |17.0       |(1,[0],[1.0])|(17,[],[])     |0.0    |[0.0]  |[0.0]     |(19,[0],[1.0])              |
|110289|女    |32 |126.net     |0.0         |3.0        |(1,[0],[1.0])|(17,[3],[1.0]) |2.0    |[2.0]  |[0.4]     |(19,[0,4,18],[1.0,1.0,0.4]) |
|110300|女    |98 |163.com     |0.0         |5.0        |(1,[0],[1.0])|(17,[5],[1.0]) |5.0    |[5.0]  |[1.0]     |(19,[0,6,18],[1.0,1.0,1.0]) |
|110287|女    |72 |123.com     |0.0         |17.0       |(1,[0],[1.0])|(17,[],[])     |5.0    |[5.0]  |[1.0]     |(19,[0,18],[1.0,1.0])       |
|110309|男    |70 |github.com  |1.0         |13.0       |(1,[],[])    |(17,[13],[1.0])|5.0    |[5.0]  |[1.0]     |(19,[14,18],[1.0,1.0])      |
|110307|男    |94 |139.net     |1.0         |10.0       |(1,[],[])    |(17,[10],[1.0])|5.0    |[5.0]  |[1.0]     |(19,[11,18],[1.0,1.0])      |
|110308|男    |31 |ask.com     |1.0         |1.0        |(1,[],[])    |(17,[1],[1.0]) |2.0    |[2.0]  |[0.4]     |(19,[2,18],[1.0,0.4])       |
|110305|男    |56 |126.net     |1.0         |3.0        |(1,[],[])    |(17,[3],[1.0]) |4.0    |[4.0]  |[0.8]     |(19,[4,18],[1.0,0.8])       |
+------+------+---+------------+------------+-----------+-------------+---------------+-------+-------+----------+----------------------------+
     */

    //  将features列SparseVector转换为string,保留将文章ID和其对应的特征向量两列
    val baseFeatureDF = featuresDF
      .withColumn("features",  FeatureUDF.vector2Str($"features"))
      .select("uid", "features")

    // 将特征向量保存一份到到HDFS
    baseFeatureDF.write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dwb_news.user_base_vector")

    // 将特征向量存入到HBASE,先将其换换为HFile
    val hBaseUtil = HBaseUtil(ss, params.hbaseZK, params.hbaseZKPort)
    log.warn("start gen hfile, load user base feature result to hbase ! ")
    // 将用户特征的DataFrame转换为HFile RDD
    val featureHFileRDD = modelData.userBaseFeatureDF2HFile(baseFeatureDF)
    // HFile RDD 生成文件后直接加载到HBASE中
    hBaseUtil.loadHfileRDD2Hbase(featureHFileRDD, params.tableName, params.hfileTmpPath)
    ss.stop()
    log.warn("job success! ")

  }

}
