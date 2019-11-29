from __future__ import print_function                                           
                                                                     
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StructField 
from pyspark.sql.types import StructType
from pyspark.sql.functions import mean
                                          
                                                                                
if __name__ == "__main__":                                                      
                                                
                                                                                
  spark = SparkSession \
    .builder \
    .appName("BasicCSVStreamAnalyzer") \
    .getOrCreate()                                                          
                                                                                
  # Create DataFrame representing the stream of CSVs
  # We define the schema for this test simply to be doubles for the 960 entries.

  userSchema = StructType([StructField("V"+str(i), DoubleType(), False) for i in range(0,960)])
  csvDF = spark \
    .readStream \
    .option("sep", ",") \
    .schema(userSchema) \
    .csv("/opt/spark-data/beamformed")

  meanV1 = csvDF.groupBy().mean("V1")      
                                                     
    # Start running the query that prints the running counts to the console     
  query = meanV1 \
    .writeStream \
    .outputMode('complete') \
    .format('console') \
    .start()                                                                
                                
  query.awaitTermination()                            
