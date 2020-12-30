//3-2 
val people2=spark.sql("select age_range,count(*) as num from (select distinct a.user_id,age_range from table_log a,table_info b where a.user_id=b.user_id and a.action_type='2' and age_range in('1','2','3','4','5','6','7','8')) group by age_range order by age_range") 
people2.show 
// //下面显示ratio(但不是sql语句 
people2.agg("num"-> "sum").show //结果为329039 
people2.withColumn("ratio",people2("num")/329039).show 