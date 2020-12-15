// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6335006167677780/2734827714634576/6106162071531468/latest.html
val rdd = sc.textFile("/FileStore/tables/Property_data.csv")
val data = rdd.filter(row => !row.contains("Price"))

// COMMAND ----------

val kvdata = data.map(x=> (x.split(",")(3).toInt,(1,x.split(",")(2).toDouble)))

// COMMAND ----------

val reducedrdd = kvdata.reduceByKey( (x,y) => (x._1 + y._1 , x._2 + y._2) )

reducedrdd.collect()



// COMMAND ----------

val finalrdd = reducedrdd.mapValues( data => data._2 / data._1)
finalrdd.collect()

for((bedroom, avg) <- finalrdd.collect() ) println(bedroom + " : " + avg)

finalrdd.saveAsTextFile("PropertyFinal.csv")



// COMMAND ----------



// COMMAND ----------


