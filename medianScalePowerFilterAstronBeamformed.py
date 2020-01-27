from __future__ import print_function                                           
                                                                     
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StructField 
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
#from pyspark.sql.functions import window
#from pyspark.sql.functions import expr
#from pyspark.sql.functions import hour
#from pyspark.sql.functions import date_trunc
from pyspark.sql.functions import udf, array

                                                                               
if __name__ == "__main__":                                                      
                                                
                                                                                
  spark = SparkSession \
    .builder \
    .appName("beamformedFiltering") \
    .getOrCreate()                                                          
                                                           
  # Create DataFrame representing the stream of CSVs
  # We will define the schema based on the metadata
  # The last 3 entries consist of a the time in second from the start of the observation, the timestamp, and the timestamp with seconds and smaller time units dropped.

  variableNames = ["V"+str(i) for i in range(0,960)]

  beamformedFieldTypes = [StructField(v, DoubleType(), False) for v in variableNames]
  beamformedFieldTypes.append(StructField("secondAfterMeasurement", DoubleType(), False))
  beamformedFieldTypes.append(StructField("beamformedTimestamp", TimestampType(), False))
  #beamformedFieldTypes.append(StructField("hourlyBeamformedTimestamp", TimestampType(), False))
  beamformedSchema = StructType(beamformedFieldTypes) 
  beamformedDF = spark \
    .readStream \
    .option("sep", ",") \
    .option("header", "true") \
    .schema(beamformedSchema) \
    .csv("/opt/spark-data/beamformed") \
    .withWatermark("beamformedTimestamp", "5 seconds")
 
  sumOfSquaresUdf = udf(lambda arr: sum(pow(a,2) for a in arr), DoubleType())

  beamformedDF = beamformedDF.withColumn('sumOfSquares', sumOfSquaresUdf(array(variableNames)))
  

  def foreach_write(df, epoch_id):
    dataDF = df.select(variableNames).toPandas()
    bfTimestamp = df.select("beamformedTimestamp").toPandas()
    bfSecondsAfterMeasurement = df.select("secondAfterMeasurement").toPandas()
    sumOfSquares = df.select("sumOfSquares").toPandas()

    writeColumns = variableNames + ['sumOfSquares', "beamformedTimestamp"]

    median = dataDF.median() #transpose to save each median in a separate column

    scaledDF = dataDF.divide(median)  
    
    scaledDF["secondAfterMeasurement"] = bfSecondsAfterMeasurement 
    scaledDF["beamformedTimestamp"] = bfTimestamp
    scaledDF["sumOfSquares"] = sumOfSquares  
    scaledDF = scaledDF.sort_values("secondAfterMeasurement")

    scaledDF.to_csv("/opt/spark-results/median_scaled_data/scaled_data" + str(epoch_id) + ".csv", header=True, index=False, columns=writeColumns)
    median.to_frame().T.to_csv("/opt/spark-results/medians/median" + str(epoch_id) + ".csv", header=True, index=False)

  query = beamformedDF.writeStream.foreachBatch(foreach_write).start()
              
  query.awaitTermination()                            
