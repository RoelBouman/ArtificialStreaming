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
from pyspark.sql.functions import hour
from pyspark.sql.functions import date_trunc

                                                                               
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
    .csv("/opt/spark-data/beamformed") \
    .withWatermark("beamformedTimestamp", "5 seconds")

  exprs = [expr('percentile_approx('+x+', 0.5)').alias('med_'+x) for x in beamformedDF.columns[:3]] 

  test_columns = ("V0", "V1", "V2", "beamformedTimestamp")

  testDF = beamformedDF.select(*test_columns)
    
  def foreach_test_write(df, epoch_id):
    dataDF = df.select("V0", "V1", "V2").toPandas()

    median = dataDF.median()

    scaledDF = dataDF.divide(dataDF.median())  

    scaledDF.to_csv("/opt/spark-data/test/test" + str(epoch_id) + ".csv")
    median.to_csv("/opt/spark-data/test/median" + str(epoch_id) + ".csv")
  

  query = testDF.writeStream.foreachBatch(foreach_test_write).start()
              
  query.awaitTermination()                            
