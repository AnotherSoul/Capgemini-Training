# Databricks notebook source
"""" https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6335006167677780/2009001189767089/6106162071531468/latest.html"""

#/FileStore/tables/ad.csv

from pyspark.sql import *
dfad = spark.read.csv("/FileStore/tables/ad.csv",header=True,inferSchema=True)

# COMMAND ----------

dfad.show()

# COMMAND ----------

dfad.registerTempTable('adTable')

# COMMAND ----------

sqlContext.sql('select * from adTable').show()

# COMMAND ----------

dfcolor_2018 = dfad.select('channel','advertiser').filter((dfad['channel']=='color') & (dfad['year']==2018))
dfstarplus_2018 = dfad.select('channel','advertiser').filter((dfad['channel']=='star-plus') & (dfad['year'] == 2018))
dfcolor_2019 = dfad.select('channel','advertiser').filter((dfad['channel']=='color') & (dfad['year']==2019))
dfstarplus_2019 = dfad.select('channel','advertiser').filter((dfad['channel']=='star-plus') & (dfad['year'] == 2019))


dfcolor_2018.show()
dfstarplus_2018.show()
dfcolor_2019.show()
dfstarplus_2019.show()

# COMMAND ----------

color_lost_2019 = dfcolor_2018.exceptAll(dfcolor_2019).withColumnRenamed('advertiser','lost advertiser')
color_new_2019  = dfcolor_2019.exceptAll(dfcolor_2018).withColumnRenamed('advertiser','new advertiser')

# COMMAND ----------

starplus_lost_2019 = dfstarplus_2018.exceptAll(dfstarplus_2019).withColumnRenamed('advertiser','lost advertiser')
starplus_new_2019  = dfstarplus_2019.exceptAll(dfstarplus_2018).withColumnRenamed('advertiser','new advertiser')

# COMMAND ----------

starplus_lost_2019.show()
starplus_new_2019.show()
color_lost_2019.show()
color_new_2019.show()

# COMMAND ----------

from pyspark.sql.types import StructField, StructType,StringType
from pyspark.sql.dataframe import 
LostFoundSchema = StructType([StructField('channel',StringType(),True),
                              StructField('advertiser_lost',StringType(),True),
                              StructField('advertiser_found',StringType(),True)])

# COMMAND ----------

from pyspark.sql.functions import lit
df1 = starplus_lost_2019.join(starplus_new_2019,'channel','leftouter')
df2 = color_lost_2019.join(color_new_2019,'channel','inner')
dfLostFound = df1.union(df2).withColumn('year',lit(2019))

# COMMAND ----------


dfLostFound.select('year','channel','lost advertiser','new advertiser').show()

# COMMAND ----------


