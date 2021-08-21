package com.qf.bigdata.recommend.udfs

import org.apache.commons.lang3.StringUtils
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/**
  * @Description: 为数据特征转换提供函数
  * @Author: QF    
  * @Date: 2020/8/3 6:10 PM   
  * @Version V1.0 
  */
object FeatureUDF {


  /**
    * 多个Vector String列合并为一列，例如
    * "[1,2]" "[4,5]"
    * 合并后 [1,2,4,5]
    * @param row ROW eg: struct($"col1",$"col2")
    * @return string
    */
  def mergeRow(row: Row): String = {

    row.toSeq.foreach(line=>StringUtils.strip(line.toString,"[]"))
    val res = row.toSeq.foldLeft("")((x,y) => StringUtils.strip(x,"[]")+ ","+StringUtils.strip(y.toString,"[]")).substring(1)
    "["+res+"]"
  }
  // 多个Vector String列合并为一列
  val mergeCols = udf(mergeRow _)

  // 向量转array字符串
  val vector2Str = udf((vecStr: org.apache.spark.ml.linalg.Vector) => {
    vecStr.toDense.toString()
  })

  // 向量字符串转向量求向量维度
   val  vecStr2Size = udf((vecStr: String) => {
    Vectors.parse(vecStr).asML.size
  })

  // 向量字符串转向量
  val vecStr2Vec = udf((vecStr: String) => {
    Vectors.parse(vecStr).asML
  })

}
