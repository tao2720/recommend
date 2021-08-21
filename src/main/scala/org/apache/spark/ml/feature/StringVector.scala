package org.apache.spark.ml.feature

import org.apache.commons.lang3.StringUtils
import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.language.existentials

/**
  * @Description:  自定义向量字符串转向量的Transformer, 只对输入的vector string列做转换，且输入列必须有列名`features`
  * @Author: QF    
  * @Date: 2020/7/31 5:52 PM   
  * @Version V1.0 
  */

class StringVector(override val uid: String) extends Transformer with HasInputCols with HasOutputCol{

  def setInputCol(value: Array[String]): this.type = set(inputCols, value)

//  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def this() = this(Identifiable.randomUID("VectorStringTransformer "))

  override def copy(extra: ParamMap): StringVector  = {
    defaultCopy(extra)
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {

    schema.add(StructField($(outputCol), new VectorUDT, false))
  }


  override def transform(df: Dataset[_]):DataFrame = {
    // 向量字符串转向量, 对于sparse 和dense 的字符串格式都支持
    val string2vector = (x: String) => {
      org.apache.spark.mllib.linalg.Vectors.parse(x).asML
    }
    val str2vec = udf(string2vector)

    // 另一种转换dense vector 字符串的方式
//    val string2vector = (x: String) => {
//      val a = StringUtils.strip(x,"[]").split(",").map(i => i.toDouble)
//      org.apache.spark.ml.linalg.Vectors.dense(a)
//    }
//    val str2vec = udf(string2vector)
//    df.withColumn($(outputCol), str2vec(df($(inputCols)(0))))
    df.withColumn($(outputCol), str2vec(col("features")))

  }


}