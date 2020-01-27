from __future__ import print_function                                           
                                                                     
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StructField 
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from pyspark.sql.functions import window
from pyspark.sql.functions import expr

                                                                               
if __name__ == "__main__":                                                      
                                                
                                                                                
  spark = SparkSession \
    .builder \
    .appName("beamformedFiltering") \
    .getOrCreate()                                                          
                                                                                
  # Create DataFrame representing the stream of CSVs
  # We will define the schema based on the metadata
  # The last 3 entries consist of a the time in second from the start of the observation, the timestamp, and the timestamp with seconds and smaller time units dropped.

  beamformedFieldTypes = [StructField("V"+str(i), DoubleType(), False) for i in range(0,960)]
  beamformedFieldTypes.append(StructField("secondAfterMeasurement", DoubleType(), False))
  beamformedFieldTypes.append(StructField("beamformedTimestamp", TimestampType(), False))
  beamformedFieldTypes.append(StructField("hourlyBeamformedTimestamp", TimestampType(), False))
  beamformedSchema = StructType(beamformedFieldTypes) 
  beamformedDF = spark \
    .readStream \
    .option("sep", ",") \
    .option("header", "true") \
    .schema(beamformedSchema) \
    .csv("/opt/spark-data/beamformed") 

  exprs = [expr('percentile_approx('+x+', 0.5)').alias('med_'+x) for x in beamformedDF.columns[:3]] 

  medianDF = beamformedDF.withWatermark("beamformedTimestamp", "5 seconds") \
    .groupBy(
      window("beamformedTimestamp", "5 seconds", "5 seconds") \
  ).agg(*exprs)

  # Start running the query that prints the running counts to the console     
  query = medianDF \
    .writeStream \
    .outputMode('Append') \
    .format('console') \
    .start()                                                                
                                
  query.awaitTermination()                            
