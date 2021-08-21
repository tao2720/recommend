package com.qf.bigdata.recommend

import java.io.FileOutputStream

import com.qf.bigdata.recommend.conf.Config
import com.qf.bigdata.recommend.transformer.{LRModelData, UserBaseFeatureModelData, VectorSchema}
import com.qf.bigdata.recommend.udfs.FeatureUDF
import com.qf.bigdata.recommend.util.SparkHelper
import javax.xml.transform.stream.StreamResult
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.StringVector
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types.IntegerType
import org.jpmml.model.JAXBUtil
import org.jpmml.sparkml.PMMLBuilder
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * @Description: iris数据集上运行LR分类模型
  * @Author: QF    
  * @Date: 2020/8/3 10:18 PM   
  * @Version V1.0 
  */
object IrisLRClassify {

  private val log = LoggerFactory.getLogger("lr-iris")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "root")

    // 解析命令行参数
    val params = Config.parseConfig(IrisLRClassify, args)
    log.warn("job running please wait ... ")
    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "lr-iris")
    import ss.implicits._

    val sourceData = ss.read.option("header",true).csv(params.irisPath)
    val transDF = sourceData.withColumn("label",col("label").cast(IntegerType))
      .filter($"label"===0 || $"label" ===1)
      transDF.show(false)

    // 读取features列
    val mergeDF = transDF.withColumn("features",FeatureUDF.mergeCols(struct($"features")))

    mergeDF.show(false)
    // mergeDF合并后的列features, 是一个Vector的字符串形式，将其vector每一个值转换为一个列，获取对应的schema
    // 只有生成pmml 时需要使用这个schema
    val  schema = VectorSchema.apply.getVectorSchemaByStrColumns(mergeDF,Array("features"))
    // 获取schema 对应的列名数组
    val  columns = schema.map(line=>line.name)

    // 自定义的字符串转Vector的transformer，jpmml没有这个转换，读取pmml时还需要另行自定义
    val stringVector = new StringVector()
      .setInputCol(columns.toArray)
      .setOutputCol("features_vec")

    // 定义逻辑回个模型
    val lr = new LogisticRegression()
      // 是否使用带截距的回归
      .setFitIntercept(true)
      // 最大迭代次数
      .setMaxIter(100)
      // 正则化系数, 值越大表示对模型训练数据集拟合系数惩罚越强,模型系数越接近或者等于0
      // 这样模型就越简单，防止模型过拟合，但越大的值可能会造成欠拟合，默认0
      .setRegParam(0)
      // 模型收敛的容忍系数,模型每次迭代比较阈值确定是否结束迭代和MaxIter参数一起控制迭代次数
      // 值越小，执行的迭代次数越多，默认值1E-6
      .setTol(1E-6)
      // 是否对输入数据做标准化处理
      .setStandardization(true)
      // 输入样本数据列
      .setFeaturesCol("features_vec")
      // 输入的标签列
      .setLabelCol("label")

    // 定义一个Pipeline,将各个特征转换及模型操作放入到其中处理
    val pipeline = new Pipeline()
      .setStages(Array(stringVector,lr))

    // 将数据集itemFeatureDF，就是我们的文章内容信息数据作用到我们定义的pipeline上
    val pipelineModel = pipeline.fit(mergeDF)

    // 获取pipeline中训练好的逻辑回归模型
    val lrModel = pipelineModel.stages(1).asInstanceOf[LogisticRegressionModel]
    // 打印模型评估指标
    val modelData = LRModelData(ss,params.env)
    modelData.printSummary(lrModel)
  }
}
