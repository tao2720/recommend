package com.qf.bigdata.recommend.udfs

import java.text.ParseException
import java.util
import java.util.{ArrayList, List}

import com.qf.bigdata.recommend.contant.{Action, Constant}
import com.qf.bigdata.recommend.util.DateUtil

/**
  * @Description: 给用户行为打分的UDF，考虑时间衰减
  * @Author: QF    
  * @Date: 2020/7/16 9:45 PM   
  * @Version V1.0 
  */
object RatingUDF {

  /**
    * 通过用户行为，和该行为发生的时间距离当前时间的长短，赋予行为一个评分
    * @param action
    * @param date
    */
  def action2rating(action:String,date:String):Float={
    // 行为权重 * 时间权重
    val rating = getActionWeight(action) * getSigmoidTimeWeight(date)
    rating
  }


  /**
    * 定义一个行为权重函数，对每一个行为按照重要程度赋值一个权重
    * @param action
    * @return
    */
  def getActionWeight(action:String ): Float={
    Action.withNameOpt(action).getOrElse() match  {
      case Action.Click=>  0.1f
      case Action.Share=> 0.15f
      case Action.Comment=> 0.2f
      case Action.Collect => 0.25f
      case Action.Like => 0.3f
      case _ =>  0.0f
    }

  }


  /**
    * 注： 如果实在不理解这个没有关系，可以不加入时间衰减因素
    * 时间衰减函数，表示了一个行为发生的时间距离当前时间越远，那这个行为的权重就应该较低，相当于给行为加上了时间权重
    * Sigmoid 归一化时间权重，将权重值归一到 0~1 之间
    * Sigmoid = 1 /(1 + Math.exp(1.0-x));
    * w = Sigmoid((AGING_TIME-x-7)*0.8) 条件；[AGING_TIME-x<0 时,AGING_TIME-x=1] 约值域：(0.03,0.98)
    * AGING_TIME=14 如果调整AGING_TIME常量值，需要同时调整权重函数
    */
  def getSigmoidTimeWeight(date: String): Float = {
    try { // 获取时间隔
      var interval = Constant.ARTICLE_AGING_TIME - DateUtil.diffDayToNow(date)
      if (interval < 0) interval = 1
      val x = interval.toDouble - 7
      return Sigmoid(x * 0.8).toFloat
    } catch {
      case e: ParseException =>
        e.printStackTrace()
    }
    0.0f
  }

  /**
    * Sigmoid 归一化函数
    *
    * @param x
    * @return
    */
  private def Sigmoid(x: Double) = 1 / (1 + Math.exp(1.0 - x))


}
