package com.qf.bigdata.recommend.contant

/**
  * @Description: 定义常量
  * @Author: QF    
  * @Date: 2020/7/16 10:01 PM   
  * @Version V1.0 
  */
object Constant {

  // 新闻文章的时效时间，当前前按照15天计算，表示文章在15天内具有更大的阅读价值，且时间越远价值越低
  // 如果实在不理解这个没有关系，可以不加入时间衰减因素
  val ARTICLE_AGING_TIME = 14
}
