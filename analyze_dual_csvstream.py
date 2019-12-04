from __future__ import print_function                                           
                                                                     
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StructField 
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import expr
                                          
                                                                                
if __name__ == "__main__":                                                      
                                                
                                                                                
  spark = SparkSession \
    .builder \
    .appName("DualCSVStreamAnalyzer") \
    .getOrCreate()                                                          
                                                                                
  # Create DataFrame representing the stream of CSVs
  # We define the schema for this test simply to be doubles for the 960 entries.
  # The last entry is a timestamp

  beamformedFieldTypes = [StructField("V"+str(i), DoubleType(), False) for i in range(0,960)]
  beamformedFieldTypes.append(StructField("beamformedtimestamp", TimestampType(), False))
  beamformedFieldTypes.append(StructField("hourly_beamformedtimestamp", TimestampType(), False))
  beamformedSchema = StructType(beamformedFieldTypes) 
  beamformedDF = spark \
    .readStream \
    .option("sep", ",") \
    .option("header", "true") \
    .schema(beamformedSchema) \
    .csv("/opt/spark-data/beamformed") 
 
  weatherFieldTypes = [StructField("V"+str(i), DoubleType(), True) for i in range(0,22)]
  weatherFieldTypes.append(StructField("weathertimestamp", TimestampType(), False))
  weatherSchema = StructType(weatherFieldTypes) 
  weatherDF = spark \
    .readStream \
    .option("sep", ",") \
    .option("header", "true") \
    .schema(weatherSchema) \
    .csv("/opt/spark-data/weather")

  # Watermark data
  watermarkedBeamformedDF = beamformedDF.withWatermark("beamformedtimestamp", "1 hours")
  watermarkedWeatherDF = weatherDF.withWatermark("weathertimestamp", "2 hours")

  # Joining dataframes
  joinedDF = watermarkedBeamformedDF.join(
    watermarkedWeatherDF,
    expr("""
      hourly_beamformedtimestamp = weathertimestamp AND
      beamformedtimestamp >= weathertimestamp AND
      beamformedtimestamp <= weathertimestamp + interval 1 hour
      """),
    "leftOuter")

  # Sample operation on joined data
  maxTimeStamp = joinedDF.groupby("beamformedtimestamp", "weathertimestamp").agg(max_("beamformedtimestamp"))                    

  # Start running the query that prints the running counts to the console     
  query = maxTimeStamp \
    .writeStream \
    .outputMode('complete') \
    .format('console') \
    .start()                                                                
                                
  query.awaitTermination()                            
