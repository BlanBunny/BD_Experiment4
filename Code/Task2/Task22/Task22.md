#scala

//2-2 统计双十一购买了商品的商家年龄段的比例

val df_info2 = df_info.select("user_id","age_range").filter("age_range>0 and age_range<9")

val df_log2 = df_log.select("user_id","time_stamp","action_type").filter("time_stamp=1111 and action_type=2")

val dfjoin2 = df_info2.join(df_log2,"user_id")

val dfcount2 = dfjoin2.groupBy("age_range").count()

dfcount2.withColumn("ratio",dfcount2("count")/dfjoin2.count).show
+---------+------+--------------------+                                         
|age_range| count|               ratio|
+---------+------+--------------------+
|        7| 19363|0.019736191647869567|
|        3|327758| 0.33407502464093547|
|        8|  3476|0.003542994482672861|
|        5|133480| 0.13605261897214427|
|        6|103774| 0.10577408211878409|
|        1|    54|5.504076584129301E-5|
|        4|268549|  0.2737248634428407|
|        2|124637| 0.12703918392891178|
+---------+------+--------------------+

/*将答案保存到本地*/
val dfratioa = dfcount2.withColumn("ratio",dfcount2("count")/dfjoin2.count).select("age_range","ratio").orderBy("age_range")

val rddra = dfratioa.rdd.map(x=>(x(0),x(1)))

val rddra = dfratioa.rdd.map(x=>(x(0),x(1))).collect()

sc.parallelize(rddra).saveAsTextFile("hdfs://localhost:9000/Task12/output2-2")
                                                                                







