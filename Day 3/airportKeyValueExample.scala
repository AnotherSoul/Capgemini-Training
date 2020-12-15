// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6335006167677780/977741722467637/6106162071531468/latest.html

// COMMAND ----------

val data = sc.textFile("/FileStore/tables/airports.text")
val rdd = data.map(line => ( line.split(",")(1), line.split(",")(3) ))
rdd.take(10)

// COMMAND ----------

val noCanadaRdd = rdd.filter( x => x._2 != "\"Canada\"")
noCanadaRdd.take(10)

// COMMAND ----------

rdd.filter(x => x._2 == "\"Canada\"").take(5)

// COMMAND ----------

val listData = List("Ashutosh 2222","Bijay 1111", "Vijay 1333")

val kvrdd = sc.parallelize(listData)

val rddnew = kvrdd.map(x => (x.split(" ")(0), x.split(" ")(1).toInt))

rddnew.collect()

// COMMAND ----------

rddnew.mapValues(x => x+10).take(3)

val data2 = data.map(line => ( line.split(",")(1), line.split(",")(11) ))


data2.mapValues(x => x.toLowerCase).take(5)

// COMMAND ----------


