#%% Import require packages
import h5py
import pandas as pd
import tarfile
import docker
import time
import re
from knmy import knmy
from timeloop import Timeloop
from datetime import timedelta
from dateutil.parser import parse
from io import BytesIO

#%% Helper functions.

def to_writeable_timestamp(timestamp):
    timestamp_string = str(timestamp)
    
    cleaned_timestamp_string = re.sub(r'[ :]', '-', re.sub(r'\+.*', '',timestamp_string))
    
    return(cleaned_timestamp_string)

def write_beamformed(data_part_df):
    data_part_string = data_part_df.to_csv(sep=",", date_format="%Y-%m-%d %H:%M:%S", index=False)
         
    tarstream = BytesIO()
    tar = tarfile.TarFile(fileobj=tarstream, mode='w')
    file_data = data_part_string.encode('utf8')
    tarinfo = tarfile.TarInfo(name="measurement"+str(measurement_index)+"-"+str(measurement_index+index_delta)+".csv")
    tarinfo.size = len(file_data)
    tarinfo.mtime = time.time()
    tar.addfile(tarinfo, BytesIO(file_data))
    tar.close()

    tarstream.seek(0)
    spark_master.put_archive("/opt/spark-data/beamformed", tarstream)

def fetch_and_write_weather(last_timestamp, last_hourly_measurement):
    
    #Correct for datetime handling in knmy function by adding hour offset
    _, _, _, knmi_df = knmy.get_hourly_data(stations=[279], start=last_hourly_measurement-timedelta(hours=1), end=last_timestamp-timedelta(hours=1), parse=True)
    knmi_df = knmi_df.drop(knmi_df.index[0]) #drop first row, which contains a duplicate header
    
    knmi_df["timestamp"] = [(parse(date) + timedelta(hours=int(hour))) for date, hour in zip(knmi_df["YYYYMMDD"], knmi_df["HH"])]
    knmi_df = knmi_df.drop(["STN", "YYYYMMDD", "HH"], axis=1)
    
    weather_string = knmi_df.to_csv(sep=",", date_format="%Y-%m-%d %H:%M:%S", index=False)
    
    filename = "weather"+to_writeable_timestamp(last_hourly_measurement)+"-to-"+to_writeable_timestamp(last_timestamp)+".csv"
    tarstream = BytesIO()
    tar = tarfile.TarFile(fileobj=tarstream, mode='w')
    file_data = weather_string.encode('utf8')
    tarinfo = tarfile.TarInfo(name=filename)
    tarinfo.size = len(file_data)
    tarinfo.mtime = time.time()
    tar.addfile(tarinfo, BytesIO(file_data))
    tar.close()

    tarstream.seek(0)
    spark_master.put_archive("/opt/spark-data/weather", tarstream)
    
#%% initialize variables and initialize hdf5 access and docker containers
filename = 'L701913_SAP000_B000_S0_P000_bf.h5'

h5 = h5py.File(filename, "r")
stokes = h5["/SUB_ARRAY_POINTING_000/BEAM_000/STOKES_0"]

client = docker.from_env()
spark_master = client.containers.get("spark-master")

tl = Timeloop()

time_start = parse(h5.attrs['OBSERVATION_START_UTC']) #Start of measurements as datetime object
measurement_index = 102984 #Which measurement should streaming start with?
# For when to test the hourly change: 
# 18*60*(10**6)/time_delta.microseconds
# Out[172]: 102994.46881556361 
# with 102984 first batch should have weather from hour 11, second batch from hour 12
index_delta = 10 #How many measurements should be sent each second
time_delta = timedelta(seconds=h5["/SUB_ARRAY_POINTING_000/BEAM_000/COORDINATES/COORDINATE_0"].attrs["INCREMENT"]) #The time between two consecutive measurements


# calculate first timestamp and set it to 1 hour before measurements start
last_hourly_measurement = (time_start - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
#%% define jobs
@tl.job(interval=timedelta(seconds=5))
def read_and_write():
    global measurement_index   
    global last_hourly_measurement
    
    #measurements -and- timestamp
    data_part_df = pd.DataFrame(stokes[measurement_index:(measurement_index+index_delta),:])
    data_part_df["timestamp"] = [time_start+dt for dt in [d*time_delta for d in range(measurement_index, measurement_index+index_delta)]]
    data_part_df["hourly_timestamp"] = [(time_start+dt).replace(minute=0,second=0, microsecond=0) for dt in [d*time_delta for d in range(measurement_index, measurement_index+index_delta)]]


    #Determine if new weather data should be written
    if any(t.replace(minute=0, second=0, microsecond=0) > last_hourly_measurement for t in data_part_df["timestamp"]):
         #write weather data
         last_timestamp = data_part_df["timestamp"].iloc[-1].replace(minute=0, second=0, microsecond=0)
         fetch_and_write_weather(last_timestamp, last_hourly_measurement)
         
         last_hourly_measurement = last_timestamp
         
    #write beamformed data
    write_beamformed(data_part_df)
    
    print("Written measurements "+str(measurement_index)+"-"+str(measurement_index+index_delta))
    measurement_index=measurement_index+index_delta    
    
#%% start jobs
tl.start(block=True)