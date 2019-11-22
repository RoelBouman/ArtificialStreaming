
#%% Import require packages
import h5py
import numpy as np
import tarfile
import docker
import time
from timeloop import Timeloop
from datetime import timedelta
from io import BytesIO

#%% Loading data
filename = 'L701913_SAP000_B000_S0_P000_bf.h5'

h5 = h5py.File(filename, "r")

#%% Writing data to csv and tar
stokes = h5["/SUB_ARRAY_POINTING_000/BEAM_000/STOKES_0"]

client = docker.from_env()
spark_master = client.containers.get("spark-master")

tl = Timeloop()

measurement_index = 0

@tl.job(interval=timedelta(seconds=5))
def read_and_write():
    global measurement_index
    data_part = stokes[measurement_index:(measurement_index+10),:]
    np.savetxt("tempdata/foo.csv", data_part, delimiter=",")

    data_part_string = open("tempdata/foo.csv", "r").read()
         
    tarstream = BytesIO()
    tar = tarfile.TarFile(fileobj=tarstream, mode='w')
    file_data = data_part_string.encode('utf8')
    tarinfo = tarfile.TarInfo(name="measurement"+str(measurement_index)+"-"+str(measurement_index+10)+".csv")
    tarinfo.size = len(file_data)
    tarinfo.mtime = time.time()
    tar.addfile(tarinfo, BytesIO(file_data))
    tar.close()

    tarstream.seek(0)
    spark_master.put_archive("/opt/spark-data/beamformed", tarstream)
    
    measurement_index=measurement_index+10
    print("Written"+"measurement"+str(measurement_index)+"-"+str(measurement_index+10))
    
tl.start(block=True)







