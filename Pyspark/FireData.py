# Databricks notebook source
"""" https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6335006167677780/3310173277756366/6106162071531468/latest.html """
  
import urllib
 
ACCESS_KEY = "AKIA4WR7LEUPDH5NNOXG"
SECRET_KEY = "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"
ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY, "")
 
 
AWS_BUCKET_NAME = "firedataregex"
MOUNT_NAME = "s3FireData1"
 
dbutils.fs.mount("s3n://%s:%s@%s" %  (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" %  MOUNT_NAME )

# COMMAND ----------

display(dbutils.fs.ls("/mnt/s3FireData1"))

# COMMAND ----------

dfFire = spark.read.csv("dbfs:/mnt/s3FireData1/Fire_Department_Calls_for_Service.csv", header=True, inferSchema=True)
display(dfFire)

# COMMAND ----------

dfFire.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType

# COMMAND ----------

fire_schema =            StructType([StructField('CallNumber',StringType(),True),
                         StructField('UnitID',StringType(),True),
                         StructField('IncidentNumber',IntegerType(),True),
                         StructField('CallType',StringType(),True),
                         StructField('CallDate',StringType(),True),
                         StructField('WatchDate',StringType(),True),
                         StructField('ReceivedDtTm',StringType(),True),
                         StructField('EntryDtTm',StringType(),True),
                         StructField('DispatchDtTm',StringType(),True),
                         StructField('ResponseDtTm',StringType(),True),
                         StructField('OnSceneDtTm',StringType(),True),
                         StructField('TransportDtTm',StringType(),True),
                         StructField('HospitalDtTm',StringType(),True),
                         StructField('CallFinalDisposition',StringType(),True),
                         StructField('AvailableDtTm',StringType(),True),
                         StructField('Address',StringType(),True),
                         StructField('City',StringType(),True),
                         StructField('ZipcodeofIncident',IntegerType(),True),
                         StructField('Battalion',StringType(),True),
                         StructField('StationArea',StringType(),True),
                         StructField('Box',StringType(),True),
                         StructField('OriginalPriority',StringType(),True),
                         StructField('Priority',StringType(),True),
                         StructField('FinalPriority',IntegerType(),True),
                         StructField('ALSUnit',BooleanType(),True),
                         StructField('CallTypeGroup',StringType(),True),
                         StructField('NumberofAlarms',IntegerType(),True),
                         StructField('UnitType',StringType(),True),
                         StructField('UnitsequenceInCallDispatch',IntegerType(),True),
                         StructField('FirePreventionDistrict',StringType(),True),
                         StructField('SupervisorDistrict',StringType(),True),
                         StructField('NeighborhooodsAnalysisBoundaries',StringType(),True),
                         StructField('Location',StringType(),True),
                         StructField('RowID',StringType(),True),
                         StructField('shape',StringType(),True),
                         StructField('SupervisorDistricts',IntegerType(),True),
                         StructField('FirePreventionDistricts',IntegerType(),True),
                         StructField('CurrentPoliceDistricts',IntegerType(),True),
                         StructField('NeighborhoodsAnalysisBoundaries',IntegerType(),True),
                         StructField('ZipCodes',IntegerType(),True),
                         StructField('NeighborhoodsoOld',IntegerType(),True),
                         StructField('PoliceDistricts',IntegerType(),True),
                         StructField('CivicCenterHarmReductionProjectBoundary',IntegerType(),True),
                         StructField('HSOCZones',IntegerType(),True),
                         StructField('CentralMarket',IntegerType(),True),
                         StructField('Neighborhoods',IntegerType(),True),
                         StructField('SFFindNeighborhoods',IntegerType(),True),
                         StructField('CurrentPoliceDistricts2',IntegerType(),True),
                         StructField('CurrentSupervisorDistricts',IntegerType(),True)])

# COMMAND ----------

dfNew = spark.read.csv("dbfs:/mnt/s3FireData1/Fire_Department_Calls_for_Service.csv", header=True, schema=fire_schema )

# COMMAND ----------

display(dfNew.limit(20))
dfNew.count()

# COMMAND ----------

dfNew.select("CallType").distinct().show()

# COMMAND ----------

dfNew.select("CallType").distinct().show(20,False)

# COMMAND ----------

display( dfNew.select("CallType").groupBy("CallType").count().orderBy("count", ascending=False) ) 

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp

# COMMAND ----------

pattern1 = 'MM/dd/yyyy' 
pattern2 = 'MM/dd/yyyy hh:mm:ss a'
to_pattern1 = 'yyyy/MM/dd'
 
 
display( dfNew.withColumn("newCol", unix_timestamp( dfNew["CallDate"], pattern1 ).cast("timestamp")  ).drop("CallDate")\
        .withColumn("newCol2", unix_timestamp( dfNew["WatchDate"], pattern1 ).cast("timestamp")  ).drop("WatchDate")\
  .withColumn("newCol3", unix_timestamp( dfNew['ReceivedDtTm'], pattern2).cast("timestamp")) )

# COMMAND ----------

dfNew.select('ReceivedDtTm').show()

# COMMAND ----------


