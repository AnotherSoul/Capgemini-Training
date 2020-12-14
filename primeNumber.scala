// Databricks notebook source
def isPrime2(i :Int):Boolean = {
       if (i <= 1)
       false
     else if (i == 2)
       true
     else 
        (!(2 to (i-1)).exists(x => i % x == 0))
       
}
// isPrime2: (i: Int)Boolean

// COMMAND ----------

val dat = sc.textFile("/FileStore/tables/numberData.csv")
val header = dat.first() 
val data = dat.filter(row => row != header) 

val idata = data.map(x=>x.toInt)

// COMMAND ----------

val res = idata.filter(x=> isPrime2(x))

// COMMAND ----------

res.reduce(_+_)

// COMMAND ----------


