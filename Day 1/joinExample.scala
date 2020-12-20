// Databricks notebook source
 val data1 = sc.parallelize(List(("Ashutosh",2020),("Mishra",2021)))
val data2 = sc.parallelize(List(("Ashutosh","India"),("Vijay","US")))

// COMMAND ----------

data1.join(data2).collect()

// COMMAND ----------

data1.leftOuterJoin(data2).collect()

// COMMAND ----------

data1.rightOuterJoin(data2).collect()

// COMMAND ----------

data1.fullOuterJoin(data2).collect()

// COMMAND ----------

2
