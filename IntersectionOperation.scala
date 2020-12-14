// Databricks notebook source
val julyData = sc.textFile("/FileStore/tables/nasa_july.tsv")
val augustData = sc.textFile("/FileStore/tables/nasa_august.tsv")


// COMMAND ----------

val julyHost = julyData.map(x => x.split("\t")(0))
val augustHost = augustData.map(x => x.split("\t")(0))

// COMMAND ----------

var intersect = julyHost.intersection(augustHost)

// COMMAND ----------

val i = intersect.filter(line => !line.contains("host"))
val count = i.count()

// COMMAND ----------


