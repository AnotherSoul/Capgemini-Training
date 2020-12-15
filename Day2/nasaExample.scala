// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6335006167677780/241003932736326/6106162071531468/latest.html

val nasajuly= sc.textFile("/FileStore/tables/nasa_july.tsv")
val nasaaug=sc.textFile("/FileStore/tables/nasa_august.tsv")



val unionrdd=nasajuly.union(nasaaug)



val header=unionrdd.first
unionrdd.filter(line=>line!=header).count()

def headerRemove(line:String): Boolean = !{line.contains("bytes")}
val nasa = unionrdd.filter(x=>headerRemove(x))
nasa.collect()

// COMMAND ----------

nasa.sample(withReplacement = true, fraction=0.20).collect()


// Task1
val nasaSplit=nasa.map(x=>x.split("\t"))
nasaSplit.filter(x=>(x(6)).toDouble>1000 || (x(5)).toDouble==0).take(10)


// Task1
val nasaj=nasajuly.map(x=>x.split("\t")(0)).filter(x=>x!="host")
val nasa1=nasaaug.map(x=>x.split("\t")(0)).filter(x=>x!="host")
val nasacommon=nasaj.intersection(nasa1)
nasacommon.count()

// COMMAND ----------

// COMMAND ----------

// COMMAND ----------

val nasaaug = sc.textFile("FileStore/tables/nasa_august.tsv")
val nasajuly = sc.textFile("FileStore/tables/nasa_july.tsv")

// COMMAND ----------

val aughost = nasaaug.map(x => x.split("\t")(0))
val julyhost = nasajuly.map(x => x.split("\t")(0))
aughost.take(3)
julyhost.take(3)

// COMMAND ----------

aughost.filter(x => x=="host").collect()
