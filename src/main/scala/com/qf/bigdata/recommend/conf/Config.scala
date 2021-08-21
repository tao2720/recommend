package com.qf.bigdata.recommend.conf

/**
  * @Description: 解析命令行参数
  * @Author: QF
  * @Date: 2020/7/2 1:38 PM
  * @Version V1.0
  */
case class Config(
                   env: String = "",
                   topK: Int = 100,
                   hbaseZK:String ="",
                   hbaseZKPort:String ="2181",
                   hfileTmpPath:String="",
                   tableName:String ="recommend:news-cf",
                   irisPath:String =""

                 )


object Config {

  def parseConfig(obj: Object, args: Array[String]): Config = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$", "")
    val parser = new scopt.OptionParser[Config]("spark  " + programName) {
      head(programName, "1.0")
      opt[String]('e', "env").required().action((x, config) => config.copy(env = x)).text("env: dev or prod")
      programName match {
        case "ItemCF" =>
          opt[String]('z', "hbaseZK").required().action((x, config) => config.copy(hbaseZK = x)).text("hbaseZK: hbase zookeeper")
          opt[String]('p', "hbaseZKPort").optional().action((x, config) => config.copy(hbaseZKPort = x)).text("hbaseZKPort: hbase zookeeper port ,default 2181")
          opt[String]('f', "hfileTmpPath").required().action((x, config) => config.copy(hfileTmpPath = x)).text("hfileTmpPath: 数据写入hbase时,生成hfile的临时目录")
          opt[String]('t', "tableName").optional().action((x, config) => config.copy(tableName = x)).text("tableName: 数据写入hbase的表名，默认 recommend:news-cf")
          opt[Int]('k', "topK").optional().action((x, config) => config.copy(topK = x)).text("topK: 推荐结果取topK,默认100")

        case "ALSCF" =>
          opt[String]('z', "hbaseZK").required().action((x, config) => config.copy(hbaseZK = x)).text("hbaseZK: hbase zookeeper")
          opt[String]('p', "hbaseZKPort").optional().action((x, config) => config.copy(hbaseZKPort = x)).text("hbaseZKPort: hbase zookeeper port ,default 2181")
          opt[String]('f', "hfileTmpPath").required().action((x, config) => config.copy(hfileTmpPath = x)).text("hfileTmpPath: 数据写入hbase时,生成hfile的临时目录")
          opt[String]('t', "tableName").optional().action((x, config) => config.copy(tableName = x)).text("tableName: 数据写入hbase的表名，默认 recommend:news-cf")
          opt[Int]('k', "topK").optional().action((x, config) => config.copy(topK = x)).text("topK: 推荐结果取topK,默认100")
        case "ItemBaseFeature" =>
          opt[String]('z', "hbaseZK").required().action((x, config) => config.copy(hbaseZK = x)).text("hbaseZK: hbase zookeeper")
          opt[String]('p', "hbaseZKPort").optional().action((x, config) => config.copy(hbaseZKPort = x)).text("hbaseZKPort: hbase zookeeper port ,default 2181")
          opt[String]('f', "hfileTmpPath").required().action((x, config) => config.copy(hfileTmpPath = x)).text("hfileTmpPath: 数据写入hbase时,生成hfile的临时目录")
          opt[String]('t', "tableName").required().action((x, config) => config.copy(tableName = x)).text("tableName: 数据写入hbase的表名")

        case "UserBaseFeature" =>
          opt[String]('z', "hbaseZK").required().action((x, config) => config.copy(hbaseZK = x)).text("hbaseZK: hbase zookeeper")
          opt[String]('p', "hbaseZKPort").optional().action((x, config) => config.copy(hbaseZKPort = x)).text("hbaseZKPort: hbase zookeeper port ,default 2181")
          opt[String]('f', "hfileTmpPath").required().action((x, config) => config.copy(hfileTmpPath = x)).text("hfileTmpPath: 数据写入hbase时,生成hfile的临时目录")
          opt[String]('t', "tableName").required().action((x, config) => config.copy(tableName = x)).text("tableName: 数据写入hbase的表名")
        case "ArticleEmbedding" =>
          opt[String]('z', "hbaseZK").required().action((x, config) => config.copy(hbaseZK = x)).text("hbaseZK: hbase zookeeper")
          opt[String]('p', "hbaseZKPort").optional().action((x, config) => config.copy(hbaseZKPort = x)).text("hbaseZKPort: hbase zookeeper port ,default 2181")
          opt[String]('f', "hfileTmpPath").required().action((x, config) => config.copy(hfileTmpPath = x)).text("hfileTmpPath: 数据写入hbase时,生成hfile的临时目录")
          opt[String]('t', "tableName").required().action((x, config) => config.copy(tableName = x)).text("tableName: 数据写入hbase的表名")
        case "LRClassify" =>

        case "IrisLRClassify" =>
          opt[String]('i', "irisPath").required().action((x, config) => config.copy(irisPath = x)).text("irisPath: iris数据集目录")

        case "UnionFeature" =>
          opt[String]('z', "hbaseZK").required().action((x, config) => config.copy(hbaseZK = x)).text("hbaseZK: hbase zookeeper")
          opt[String]('p', "hbaseZKPort").optional().action((x, config) => config.copy(hbaseZKPort = x)).text("hbaseZKPort: hbase zookeeper port ,default 2181")
          opt[String]('f', "hfileTmpPath").required().action((x, config) => config.copy(hfileTmpPath = x)).text("hfileTmpPath: 数据写入hbase时,生成hfile的临时目录")
          opt[String]('t', "tableName").required().action((x, config) => config.copy(tableName = x)).text("tableName: 数据写入hbase的表名")

        case _ =>

      }

    }
    parser.parse(args, Config()) match {
      case Some(conf) => conf
      case None => {
        //        println("cannot parse args")
        System.exit(-1)
        null
      }
    }

  }

}
