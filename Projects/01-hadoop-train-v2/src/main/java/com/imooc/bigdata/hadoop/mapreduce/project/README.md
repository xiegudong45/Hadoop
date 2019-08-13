## 统计页面的浏览量
`select count(1) from xxx;`

一行记录做成一个固定的key，value赋值为1

## 统计各个省份的浏览量
 ```
 select province count(1) from xxx 
 group by province;
 ```
 地市信息可以通过ip解析得到 <== ip如何转换城市信息
 ip解析：收费
 
 ## 统计页面的访问量：
 把符合规则的pageId获取到，然后进行统计即可。
 
 ## 存在的问题：
 每个MR作业都去全量读取待处理的原始日志，如果数据量很大，怎么办？
 
 ETL：全量数据不方便直接进行计算，最好进行进一步处理后再进行相应的纬度统计分析
 解析出你需要的字段： ip => 城市信息
 去除不需要的字段
 需要的字段：ip/time/url/pageId/country/province/city
 
 大数据处理完以后的不数据我们现在是存放在HDFS之上
 再进一步：使用技术或者框架把处理完的结果导出到数据库
 Sqoop：把hdfs上的统计结果导出到MySQL中
 