package com.qf.bigdata.recommend

import breeze.linalg.DenseVector
import com.qf.bigdata.recommend.conf.Config
import com.qf.bigdata.recommend.hbase.HBaseUtil
import com.qf.bigdata.recommend.transformer.ItemBaseFeatureModelData
import com.qf.bigdata.recommend.udfs.FeatureUDF
import com.qf.bigdata.recommend.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{MinMaxScaler, OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.udf
import org.slf4j.LoggerFactory

/**
  * @Description:  生成文章基本特征向量，并存储到HBASE中
  * @Author: QF    
  * @Date: 2020/7/24 5:20 PM   
  * @Version V1.0 
  */
object ItemBaseFeature {
  private val log = LoggerFactory.getLogger("item-base-feature")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "ly")

    // 解析命令行参数
    val params = Config.parseConfig(ItemBaseFeature, args)
    log.warn("job running please wait ... ")
    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "item-base-feature")
    // 基础数据处理
    val modelData = ItemBaseFeatureModelData(ss, params.env)
    // 从dwb_news.article_base_info数据表中读取文章特征数据
    val itemFeatureDF = modelData.loadSourceArticleBaseInfoData()
    /**
      * 对对itemFeatureDF中的`type_name`列，做one-hot编码
      * 因为 spark OneHotEncoderEstimator 只接受数值类型列作为one-hot编码的输入，因此我们先使用 StringIndexer将type_name的中文类型类
      * 转换为对应的索引，StringIndexer 的作用举例如下
      * type_name
      * ------------
      * 娱乐
      * 情感
      * 汽车
      * 娱乐
      *
      * 执行StringIndexer后
      * type_name
      * ------------
      * 0
      * 1
      * 2
      * 0
      * 相当于每一个值有一个索引，以索引号代替数值
      *
      */
    val indexer = new StringIndexer()
      .setInputCol("type_name").setOutputCol("type_name_index")
    // 对转换为索引表示的type_name的使用one-hot编码，这里需要注意的是，转换索引操作并不影响我们one-hot编码的结果
    // 之所以做转换，是因为spark的 OneHotEncoderEstimator 只接受数值类型作为输入
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array(indexer.getOutputCol)).setOutputCols(Array("type_name_vec"))

    /** 三个数值类型特征合并为一个向量，举例如下
      *
      * article_num | img_num| pub_gap
      * -------------|--------|------------
      * 356         | 18     |4
      *
      * 对上述三列执行 VectorAssembler 后会得到如下结果
      *
      * features
      * ---------
      * [356,18,4]
      */
    val numAssembler = new VectorAssembler()
      .setInputCols(Array("article_num", "img_num", "pub_gap"))
      .setOutputCol("numFeatures")

    // 对各个列进行最小最大归一化
    val numFeaturesScaler = new MinMaxScaler()
      .setInputCol("numFeatures")
      .setOutputCol("numFeaturesScaler")

    // 合并归一化后的数值特征列[article_num,img_num,pub_gap]和one-hot后的type_name列合并为一个向量
    val assembler = new VectorAssembler()
      .setInputCols(Array("numFeaturesScaler", "type_name_vec"))
      .setOutputCol("features")

    // 定义一个Pipeline,将各个特征转换操作放入到其中处理
    val pipeline = new Pipeline()
      .setStages(Array(indexer, encoder, numAssembler, numFeaturesScaler, assembler))

    // 将数据集itemFeatureDF，就是我们的文章内容信息数据作用到我们定义的pipeline上
    val pipelineModel = pipeline.fit(itemFeatureDF)
    // 对我们的数据集执行定义的转换操作
    val featuresDF = pipelineModel.transform(itemFeatureDF)

    /*featuresDF
    +----------+-----------+-------+---------+-------+---------------+---------------+-----------------+-------------------------------------------------+--------------------------------------------------------------------+
    |article_id|article_num|img_num|type_name|pub_gap|type_name_index|type_name_vec  |numFeatures      |numFeaturesScaler                                |features                                                            |
    +----------+-----------+-------+---------+-------+---------------+---------------+-----------------+-------------------------------------------------+--------------------------------------------------------------------+
    |70243     |348        |0      |美文     |25     |2.0            |(16,[2],[1.0]) |[348.0,0.0,25.0] |[0.01709653647752395,0.0,0.875]                  |(19,[0,2,5],[0.01709653647752395,0.875,1.0])                        |
    |70245     |430        |0      |美文     |25     |2.0            |(16,[2],[1.0]) |[430.0,0.0,25.0] |[0.02112503070498649,0.0,0.875]                  |(19,[0,2,5],[0.02112503070498649,0.875,1.0])                        |
    |61518     |408        |4      |时尚     |25     |8.0            |(16,[8],[1.0]) |[408.0,4.0,25.0] |[0.02004421518054532,0.03773584905660377,0.875]  |(19,[0,1,2,11],[0.02004421518054532,0.03773584905660377,0.875,1.0]) |
    |61525     |459        |0      |美文     |25     |2.0            |(16,[2],[1.0]) |[459.0,0.0,25.0] |[0.022549742078113486,0.0,0.875]                 |(19,[0,2,5],[0.022549742078113486,0.875,1.0])                       |
    |61551     |1106       |7      |育儿     |25     |4.0            |(16,[4],[1.0]) |[1106.0,7.0,25.0]|[0.0543355440923606,0.0660377358490566,0.875]    |(19,[0,1,2,7],[0.0543355440923606,0.0660377358490566,0.875,1.0])    |
    |61579     |1532       |6      |教育     |26     |15.0           |(16,[15],[1.0])|[1532.0,6.0,26.0]|[0.07526406288381234,0.05660377358490566,0.9375] |(19,[0,1,2,18],[0.07526406288381234,0.05660377358490566,0.9375,1.0])|
    |61583     |981        |10     |游戏     |26     |6.0            |(16,[6],[1.0]) |[981.0,10.0,26.0]|[0.04819454679439941,0.09433962264150944,0.9375] |(19,[0,1,2,9],[0.04819454679439941,0.09433962264150944,0.9375,1.0]) |
    |61596     |1270       |0      |娱乐     |25     |3.0            |(16,[3],[1.0]) |[1270.0,0.0,25.0]|[0.06239253254728568,0.0,0.875]                  |(19,[0,2,6],[0.06239253254728568,0.875,1.0])                        |
    |61598     |388        |2      |娱乐     |25     |3.0            |(16,[3],[1.0]) |[388.0,2.0,25.0] |[0.01906165561287153,0.018867924528301886,0.875] |(19,[0,1,2,6],[0.01906165561287153,0.018867924528301886,0.875,1.0]) |
    |61601     |299        |4      |娱乐     |26     |3.0            |(16,[3],[1.0]) |[299.0,4.0,26.0] |[0.014689265536723164,0.03773584905660377,0.9375]|(19,[0,1,2,6],[0.014689265536723164,0.03773584905660377,0.9375,1.0])|
    |69997     |906        |20     |美食     |26     |0.0            |(16,[0],[1.0]) |[906.0,20.0,26.0]|[0.0445099484156227,0.18867924528301888,0.9375]  |(19,[0,1,2,3],[0.0445099484156227,0.18867924528301888,0.9375,1.0])  |
    |70004     |210        |1      |美食     |25     |0.0            |(16,[0],[1.0]) |[210.0,1.0,25.0] |[0.010316875460574797,0.009433962264150943,0.875]|(19,[0,1,2,3],[0.010316875460574797,0.009433962264150943,0.875,1.0])|
    |70063     |318        |7      |美食     |26     |0.0            |(16,[0],[1.0]) |[318.0,7.0,26.0] |[0.015622697126013265,0.0660377358490566,0.9375] |(19,[0,1,2,3],[0.015622697126013265,0.0660377358490566,0.9375,1.0]) |
    |70070     |658        |3      |美食     |25     |0.0            |(16,[0],[1.0]) |[658.0,3.0,25.0] |[0.0323262097764677,0.02830188679245283,0.875]   |(19,[0,1,2,3],[0.0323262097764677,0.02830188679245283,0.875,1.0])   |
    |70071     |418        |6      |美食     |25     |0.0            |(16,[0],[1.0]) |[418.0,6.0,25.0] |[0.020535494964382214,0.05660377358490566,0.875] |(19,[0,1,2,3],[0.020535494964382214,0.05660377358490566,0.875,1.0]) |
    |70077     |709        |1      |美食     |25     |0.0            |(16,[0],[1.0]) |[709.0,1.0,25.0] |[0.034831736674035864,0.009433962264150943,0.875]|(19,[0,1,2,3],[0.034831736674035864,0.009433962264150943,0.875,1.0])|
    |70098     |967        |6      |美食     |25     |0.0            |(16,[0],[1.0]) |[967.0,6.0,25.0] |[0.04750675509702776,0.05660377358490566,0.875]  |(19,[0,1,2,3],[0.04750675509702776,0.05660377358490566,0.875,1.0])  |
    |70107     |141        |2      |美食     |25     |0.0            |(16,[0],[1.0]) |[141.0,2.0,25.0] |[0.006927044952100221,0.018867924528301886,0.875]|(19,[0,1,2,3],[0.006927044952100221,0.018867924528301886,0.875,1.0])|
    |63413     |595        |4      |娱乐     |26     |3.0            |(16,[3],[1.0]) |[595.0,4.0,26.0] |[0.02923114713829526,0.03773584905660377,0.9375] |(19,[0,1,2,6],[0.02923114713829526,0.03773584905660377,0.9375,1.0]) |
    |63414     |206        |0      |娱乐     |25     |3.0            |(16,[3],[1.0]) |[206.0,0.0,25.0] |[0.010120363547040039,0.0,0.875]                 |(19,[0,2,6],[0.010120363547040039,0.875,1.0])                       |
    +----------+-----------+-------+---------+-------+---------------+---------------+-----------------+-------------------------------------------------+--------------------------------------------------------------------+
    */
    featuresDF.show(false)
    import ss.implicits._

    //  将features列SparseVector转换为string,保留将文章ID和其对应的特征向量两列
    val baseFeatureDF = featuresDF
      .withColumn("features", FeatureUDF.vector2Str($"features"))
      .select("article_id", "features")

    // 将特征向量保存一份到到HDFS
    baseFeatureDF.write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dwb_news.article_base_vector")

    // 将特征向量存入到HBASE,先将其转换为HFile
    val hBaseUtil = HBaseUtil(ss, params.hbaseZK, params.hbaseZKPort)
    log.warn("start gen hfile, load item base feature result to hbase ! ")
    // 物品特征的DataFrame转换为HFile RDD
    val featureHFileRDD = modelData.itemBaseFeatureDF2HFile(baseFeatureDF)
    // HFile RDD 生成文件后直接加载到HBASE中
    hBaseUtil.loadHfileRDD2Hbase(featureHFileRDD, params.tableName, params.hfileTmpPath)
    ss.stop()
    log.warn("job success! ")
  }
}
