// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6335006167677780/241003932736330/6106162071531468/latest.html

val data = sc.textFile("/FileStore/tables/FriendsData.csv")


val removeheader = data.filter(line => !line.contains("name"))


// COMMAND ----------

val friendrdd = removeheader.map(x => ( x.split(",")(2).toInt, (1,x.split(",")(3).toDouble) ))
friendrdd.take(5)

// COMMAND ----------

val reducedrdd = friendrdd.reduceByKey( (x,y) => (x._1 + y._1 , x._2 + y._2) )
reducedrdd.collect()

// COMMAND ----------

val finalrdd = reducedrdd.mapValues( data => data._2 / data._1 )
finalrdd.collect()

// COMMAND ----------

for((age, avg_friends) <- finalrdd.collect() ) println(age + " : " + avg_friends)

// COMMAND ----------

val maxfriendrdd = removeheader.map(x => ( x.split(",")(2).toInt, x.split(",")(3).toDouble ))
maxfriendrdd.take(5)

// COMMAND ----------

val maxrdd = maxfriendrdd.reduceByKey(math.max(_, _))
maxrdd.collect()

// COMMAND ----------


