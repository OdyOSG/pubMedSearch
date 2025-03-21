

def getSparkSession(app_name="pubMedSearch"):
  from pyspark.sql import SparkSession
  return SparkSession.builder.appName(app_name).getOrCreate()
