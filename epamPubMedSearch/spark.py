

def getSparkSession(app_name="pubMedSearch"):
  from pyspark.sql import SparkSession
  spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
  return SparkSession.builder.appName(app_name).getOrCreate()
