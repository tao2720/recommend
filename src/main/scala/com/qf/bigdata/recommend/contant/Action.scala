package com.qf.bigdata.recommend.contant

/**
  * @Description: 用户行为Enum
  * @Author: QF    
  * @Date: 2020/7/17 9:00 AM   
  * @Version V1.0 
  */
object Action  extends Enumeration {
  type Action = Value

  val Click = Value("点击")
  val Share = Value("分享")
  val Comment = Value("评论")
  val Collect = Value("收藏")
  val Like = Value("点赞")

  def showAll = this.values.foreach(println)

  def withNameOpt(s: String): Option[Value] = values.find(_.toString == s)


}
