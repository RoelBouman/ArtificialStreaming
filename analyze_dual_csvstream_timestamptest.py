from __future__ import print_function                                           
                                                                     
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StructField 
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import col
from pyspark.sql.functions import max as max_
                                          
                                                                                
if __name__ == "__main__":                                                      
                                                
                                                                                
  spark = SparkSession \
    .builder \
    .appName("DualCSVStreamAnalyzer") \
    .getOrCreate()                                                          
                                                                                
  # Create DataFrame representing the stream of CSVs
  # We define the schema for this test simply to be doubles for the 960 entries.
  # The last entry is a timestamp

  fieldTypes = [StructField("V"+str(i), DoubleType(), False) for i in range(0,960)]
  fieldTypes.append(StructField("timestamp", TimestampType(), False))
  userSchema = StructType(fieldTypes) 
  csvDF = spark \
    .readStream \
    .option("sep", ",") \
    .option("header", "true") \
    .schema(userSchema) \
    .csv("/opt/spark-data/beamformed") \
 

  # We print a running maximum timestamp
  maxTimeStamp = csvDF.groupby("timestamp").agg(max_("timestamp"))                 

  # Start running the query that prints the running counts to the console     
  query = maxTimeStamp \
    .writeStream \
    .outputMode('complete') \
    .format('console') \
    .start()                                                                
                                
  query.awaitTermination()                            
