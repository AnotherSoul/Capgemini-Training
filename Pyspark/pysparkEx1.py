# Databricks notebook source
""""https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6335006167677780/2072056232875679/6106162071531468/latest.html"""
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("FirstExample").getOrCreate()

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/FriendsData-1.csv",header = True,inferSchema = True)  


# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.describe()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

df.columns

# COMMAND ----------

df.collect()

# COMMAND ----------

df.schema

# COMMAND ----------

df.select("name","age").show()
df0 = df.select("Name","Age") 

# COMMAND ----------

df0

# COMMAND ----------

df.withColumnRenamed("friends","new friends").show()

# COMMAND ----------

df.filter(df["Age"]>50).show()

# COMMAND ----------

df.filter(df["Age"]>50).select("name","age").show()

# COMMAND ----------

df.filter(df["Age"]>50).filter(df["friends"]>100).select("name","age","friends").show()

# COMMAND ----------

df.filter((df["Age"]>50) & (df["friends"] > 100)).select("name","age","friends").show()

# COMMAND ----------

df.withColumn("New Age",df["Age"]+10).select(df["name"].alias("newName"),"age","friends").show()

# COMMAND ----------

dfAmountPerMonth = df.withColumn("TotalAmount",df["Age"]*df["Friends"]*10/12).select("name",df["age"].alias("TotalAge"),df["friends"].alias("TotalFriends"),"TotalAmount")

# COMMAND ----------

dfAmountPerMonth.show()

# COMMAND ----------

from pyspark.sql.functions import format_number,mean,min,max

# COMMAND ----------

dfAmountPerMonth.select("*",format_number(dfAmountPerMonth["TotalAmount"],2).alias("upto 2 decimal place")).show()

# COMMAND ----------

df.select(mean(df["age"])).show()

# COMMAND ----------

(df.filter(df["age"] == 53)).show()

# COMMAND ----------

df.groupBy("name").max("Age").show()

# COMMAND ----------


