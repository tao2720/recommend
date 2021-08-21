package com.qf.bigdata.recommend.transformer

import com.qf.bigdata.recommend.udfs.FeatureUDF
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf, _}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * @Description: DataFrame 的向量字符串列schema转换
  * @Author: QF    
  * @Date: 2020/8/3 2:28 PM   
  * @Version V1.0 
  */
class VectorSchema {

  /**
    * 根据指定DataFrame 的向量字符串列，将其向量的每一个值设定一个字段，即获取指定向量字符串列的Schema
    * 例如：col1 =  "[0,1,2,3]"
    * 获得到 f1,f2,f3,f4 四个列
    * @param df
    * @param arrayCols
    * @return
    */
  def getVectorSchemaByStrColumns(df:DataFrame,arrayCols: Array[String]): org.apache.spark.sql.types.StructType ={

    val featureCol = ArrayBuffer[String]()
    var a = 0
    val featureSchema = ArrayBuffer[StructField]()
    for ((colName,index) <- arrayCols.zipWithIndex){

      val outColSizeName = colName+index
      val arrayROW = df.withColumn(outColSizeName,FeatureUDF.vecStr2Size(col(colName)))
        .select(outColSizeName).head(1)
      val size = arrayROW(0).getAs[Int](outColSizeName)

      for( a <- 1 to size){
        val feature = "f"+a
        featureCol.append(feature)
        featureSchema.append(StructField(feature,DoubleType,true))
      }
    }

    StructType(featureSchema.toList)

  }
}

object VectorSchema{
  def apply: VectorSchema = new VectorSchema()
}