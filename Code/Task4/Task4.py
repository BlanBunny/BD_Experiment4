from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt
#plt.rcParams["font.sans-serif"] = "SimHei" #解决中文乱码问题
#import seaborn as sns
import random
#from sklearn.model_selection import train_test_split
#from sklearn.linear_model import LogisticRegression
#from sklearn.preprocessing import LabelEncoder
#from sklearn.metrics import accuracy_score
#from sklearn import model_selection
#from sklearn.neighbors import KNeighborsRegressor

spark=SparkSession.builder.appName('random_forest').getOrCreate()

print('-----------读取用于测得数据,检测数据质量和相关的特征。即相对数据有一定的认识，对后续进行逻辑回归训练做准备------------------')

# 读取数据
df_train=spark.read.csv('hdfs://localhost:9000/Task12/train_format1.csv',inferSchema=True,header=True)
df_test=spark.read.csv('hdfs://localhost:9000/Task12/test_format1.csv',inferSchema=True,header=True)
user_info=spark.read.csv('hdfs://localhost:9000/Task12/user_info_format1.csv',inferSchema=True,header=True)
user_log=spark.read.csv('hdfs://localhost:9000/Task12/input2/user_log_format1.csv',inferSchema=True,header=True)

#print(df_test.shape,df_train.shape)
#print(user_info.shape,user_log.shape)





'''
user_info.info()
user_info.head(10)

user_info['age_range'].replace(0.0,np.nan,inplace=True)
user_info['gender'].replace(2.0,np.nan,inplace=True)
user_info.info()


user_info['age_range'].replace(np.nan,-1,inplace=True)
user_info['gender'].replace(np.nan,-1,inplace=True)


'''
'''
fig = plt.figure(figsize = (10, 6))
x = np.array(["NULL","<18","18-24","25-29","30-34","35-39","40-49",">=50"])
#<18岁为1；[18,24]为2； [25,29]为3； [30,34]为4；[35,39]为5；[40,49]为6； > = 50时为7和8
y = np.array([user_info[user_info['age_range'] == -1]['age_range'].count(),
             user_info[user_info['age_range'] == 1]['age_range'].count(),
             user_info[user_info['age_range'] == 2]['age_range'].count(),
             user_info[user_info['age_range'] == 3]['age_range'].count(),
             user_info[user_info['age_range'] == 4]['age_range'].count(),
             user_info[user_info['age_range'] == 5]['age_range'].count(),
             user_info[user_info['age_range'] == 6]['age_range'].count(),
             user_info[user_info['age_range'] == 7]['age_range'].count() + user_info[user_info['age_range'] == 8]['age_range'].count()])
plt.bar(x,y,label='人数')
plt.legend()
plt.title('用户年龄分布')


sns.countplot(x = 'age_range', order = [-1,1,2,3,4,5,6,7,8], data = user_info)
plt.title('用户年龄分布')


sns.countplot(x='gender',order = [-1,0,1],data = user_info)
plt.title('用户性别分布')


sns.countplot(x = 'age_range', order = [-1,1,2,3,4,5,6,7,8],hue= 'gender',data = user_info)
plt.title('用户性别年龄分布')
'''


user_info['age_range'].replace(-1,np.nan,inplace=True)
user_info['gender'].replace(-1,np.nan,inplace=True)


'''
sns.countplot(x = 'age_range', order = [-1,1,2,3,4,5,6,7,8],hue= 'gender',data = user_info)
plt.title('用户性别年龄分布')
'''


user_log.head()


user_log.isnull().sum(axis=0)


user_log.isnull().sum(axis=0)


user_log.info()


df_train.head(10)

df_train.info()


user_log['time_stamp'].hist(bins = 9)


#sns.countplot(x = 'action_type', order = [0,1,2,3],data = user_log)



#df_train[df_train['label'] == 1]


#user_log[(user_log['user_id'] == 34176) & (user_log['seller_id'] == 3906)]







df_train= df_train.select("*").toPandas()
user_info=user_info.select("*").toPandas()
df_train = pd.merge(df_train,user_info,on="user_id",how="left")
df_train.head()


#example
#logs_count=log.groupby("user_id","seller_id").count()
#logs_count=logs_count.withColumnRenamed("seller_id","merchant_id")
#logs_count=logs_count.withColumnRenamed("count","logs")



total_logs_temp = user_log.groupby("user_id","seller_id").count()
total_logs_temp = total_logs_temp.withColumnRenamed("seller_id","merchant_id")
total_logs_temp=total_logs_temp.withColumnRenamed("count","total_logs")

#.reset_index()[["user_id","seller_id","item_id"]]
#total_logs_temp.head(10)

#total_logs_temp.rename(columns={"seller_id":"merchant_id","item_id":"total_logs"},inplace=True)
total_logs_temp.head()

total_logs_temp=total_logs_temp.select("*").toPandas()
df_train = pd.merge(df_train,total_logs_temp,on=["user_id","merchant_id"],how="left")
df_train.head()



unique_item_ids_temp = user_log.groupby([user_log["user_id"],user_log["seller_id"],user_log["item_id"]]).count().reset_index()[["user_id","seller_id","item_id"]]
unique_item_ids_temp.head(10)

unique_item_ids_temp1 = unique_item_ids_temp.groupby([unique_item_ids_temp["user_id"],unique_item_ids_temp["seller_id"]]).count().reset_index()
unique_item_ids_temp1.head(10)

unique_item_ids_temp1.rename(columns={"seller_id":"merchant_id","item_id":"unique_item_ids"},inplace=True)
unique_item_ids_temp1.head(10)

df_train = pd.merge(df_train,unique_item_ids_temp1,on=["user_id","merchant_id"],how="left")
df_train.head()


categories_temp = user_log.groupby([user_log["user_id"],user_log["seller_id"],user_log["cat_id"]]).count().reset_index()[["user_id","seller_id","cat_id"]]
categories_temp.head(20)

categories_temp1 = categories_temp.groupby([categories_temp["user_id"],categories_temp["seller_id"]]).count().reset_index()
categories_temp1.head(10)

categories_temp1.rename(columns={"seller_id":"merchant_id","cat_id":"categories"},inplace=True)
categories_temp1.head(10)

df_train = pd.merge(df_train,categories_temp1,on=["user_id","merchant_id"],how="left")
df_train.head(10)


browse_days_temp = user_log.groupby([user_log["user_id"],user_log["seller_id"],user_log["time_stamp"]]).count().reset_index()[["user_id","seller_id","time_stamp"]]
browse_days_temp.head(10)

browse_days_temp1 = browse_days_temp.groupby([browse_days_temp["user_id"],browse_days_temp["seller_id"]]).count().reset_index()
browse_days_temp1.head(10)

browse_days_temp1.rename(columns={"seller_id":"merchant_id","time_stamp":"browse_days"},inplace=True)
browse_days_temp1.head(10)

df_train = pd.merge(df_train,browse_days_temp1,on=["user_id","merchant_id"],how="left")
df_train.head(10)


one_clicks_temp = user_log.groupby([user_log["user_id"],user_log["seller_id"],user_log["action_type"]]).count().reset_index()[["user_id","seller_id","action_type","item_id"]]
one_clicks_temp.head(10)

one_clicks_temp.rename(columns={"seller_id":"merchant_id","item_id":"times"},inplace=True)
one_clicks_temp.head(10)

one_clicks_temp["one_clicks"] = one_clicks_temp["action_type"] == 0
one_clicks_temp["one_clicks"] = one_clicks_temp["one_clicks"] * one_clicks_temp["times"]
one_clicks_temp.head(10)

one_clicks_temp["shopping_carts"] = one_clicks_temp["action_type"] == 1
one_clicks_temp["shopping_carts"] = one_clicks_temp["shopping_carts"] * one_clicks_temp["times"]
one_clicks_temp.head(10)

one_clicks_temp["purchase_times"] = one_clicks_temp["action_type"] == 2
one_clicks_temp["purchase_times"] = one_clicks_temp["purchase_times"] * one_clicks_temp["times"]
one_clicks_temp.head(10)

one_clicks_temp["favourite_times"] = one_clicks_temp["action_type"] == 3
one_clicks_temp["favourite_times"] = one_clicks_temp["favourite_times"] * one_clicks_temp["times"]
one_clicks_temp.head(10)

four_features = one_clicks_temp.groupby([one_clicks_temp["user_id"],one_clicks_temp["merchant_id"]]).sum().reset_index()
four_features.head(10)

four_features = four_features.drop(["action_type","times"], axis=1)
df_train = pd.merge(df_train,four_features,on=["user_id","merchant_id"],how="left")
df_train.head(10)




df_train.info()
df_train.isnull().sum(axis=0)


df_train = df_train.fillna(method='ffill')
df_train.info()














print('-----------数据转换，将所有的特征值放到一个特征向量中，预测值分开.划分数据用于模型------------------')

from pyspark.ml.feature import VectorAssembler                                         #一个 导入VerctorAssembler 将多个列合并成向量列的特征转换器,即将表中各列用一个类似list表示，输出预测列为单独一列。

df_assembler = VectorAssembler(inputCols=['age_range', 'gender', 'total_logs', 'unique_item_ids', 'categories','browse_days','one_clicks','shopping_carts','purchase_times','favourite_times'], outputCol="features")
df = df_assembler.transform(df_train)
df.printSchema()

df.select(['features','label']).show(10,False)

model_df=df.select(['features','label'])                                               # 选择用于模型训练的数据
train_df,test_df=model_df.randomSplit([0.75,0.25])                                    # 训练数据和测试数据分为75%和25%

train_df.groupBy('label').count().show()
test_df.groupBy('label').count().show()









print('-----------使用随机深林进行数据训练----------------')

from pyspark.ml.classification import RandomForestClassifier

rf_classifier=RandomForestClassifier(labelCol='label',numTrees=50).fit(train_df)      # numTrees设置随机数的数量为50,还有其他参数：maxDepth 树深;返回的模型类型为：RandomForestClassificationModel

rf_predictions=rf_classifier.transform(test_df)

print('{}{}'.format('评估每个属性的重要性:',rf_classifier.featureImportances))   # featureImportances : 评估每个功能的重要性,

rf_predictions.select(['probability','label','prediction']).show(10,False)

print("------查阅pyspark api，没有发现有训练准确率的字段，所以还需要计算预测的准确率------")

from pyspark.ml.evaluation import BinaryClassificationEvaluator      # 对二进制分类的评估器,它期望两个输入列:原始预测值和标签
from pyspark.ml.evaluation import MulticlassClassificationEvaluator  # 多类分类的评估器,它期望两个输入列:预测和标签

rf_accuracy=MulticlassClassificationEvaluator(labelCol='label',metricName='accuracy').evaluate(rf_predictions)
print('MulticlassClassificationEvaluator 随机深林测试的准确性： {0:.0%}'.format(rf_accuracy))

rf_auc=BinaryClassificationEvaluator(labelCol='label').evaluate(rf_predictions)
print('BinaryClassificationEvaluator 随机深林测试的准确性： {0:.0%}'.format(rf_auc))


print('-----------保持模型，用于下次使用----------------')

rf_classifier.save("RF_model")