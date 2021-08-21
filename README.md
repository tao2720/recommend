### 推荐系统企业工程实战

#### 注意
* 请替换resources中core-site.xml hive-site.xml yarn-site.xml中相关值为你环境的值

#### 当前进度
* 完成召回层 ItemCF(基于物品)，ALS(基于模型) 协同过滤算法


### 编译部署

```
# 编译打包，线上运行
mvn clean package -DskipTests -Dscope.type=provided 
# 将编译好的JAR, recommend-1.0.0-jar-with-dependencies.jar,下载到你的服务器上
# 我放置到了 /opt/app/user-profile/ 目录下
mkdir -p /opt/app/user-profile/
# 同时也提供了一个编译好JAR，大家最好自己编译
wget -P  /opt/app/user-profile/  http://doc.yihongyeyan.com/qf/project/soft/app/recommend-1.0.0-jar-with-dependencies.jar
```