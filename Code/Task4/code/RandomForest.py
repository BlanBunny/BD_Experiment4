#jupyter notebook

# In[1]:

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('random_forest').getOrCreate()
df=spark.read.csv('hdfs://localhost:9000/Task12/feature.csv',inferSchema=True,header=True)


# In[2]:
from pyspark.ml.feature import VectorAssembler
df_assembler = VectorAssembler(inputCols=['age_range','gender','total_logs','unique_item_ids','categories','browse_days','one_clicks','shopping_carts','purchase_times','favourite_times'], outputCol="features")
df = df_assembler.transform(df)



-测试集和训练集划分比例：0.75：0.25
-随机森林基学习器数量：120
# In[3]:
model_df = df.select(['features','label'])
train_df,test_df=model_df.randomSplit([0.75,0.25]) 


# In[4]:
from pyspark.ml.classification import RandomForestClassifier
rf_classifier=RandomForestClassifier(labelCol='label',numTrees=120).fit(train_df)
rf_predictions=rf_classifier.transform(test_df)


# In[5]:
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

rf_accuracy=MulticlassClassificationEvaluator(labelCol='label',metricName='accuracy').evaluate(rf_predictions)
print('MulticlassClassificationEvaluator 随机森林测试的准确性： {0:.0%}'.format(rf_accuracy))


# In[6]:
rf_auc=BinaryClassificationEvaluator(labelCol='label').evaluate(rf_predictions)
print('BinaryClassificationEvaluator 随机森林测试的准确性： {0:.0%}'.format(rf_auc))

