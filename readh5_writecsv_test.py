
#%% Import require packages
import h5py
import numpy as np
import tarfile
import docker
import time
from io import BytesIO

#%% Loading data
filename = 'L701913_SAP000_B000_S0_P000_bf.h5'

h5 = h5py.File(filename, "r")

#%% Writing data to csv and tar
# https://gist.github.com/zbyte64/6800eae10ce082bb78f0b7a2cca5cbc2
stokes = h5["/SUB_ARRAY_POINTING_000/BEAM_000/STOKES_0"]

data_part = stokes[:10,:]

np.savetxt("tempdata/foo.csv", data_part, delimiter=",")

data_part_string = open("tempdata/foo.csv", "r").read()
         
tarstream = BytesIO()
tar = tarfile.TarFile(fileobj=tarstream, mode='w')
file_data = data_part_string.encode('utf8')
tarinfo = tarfile.TarInfo(name='foo.csv')
tarinfo.size = len(file_data)
tarinfo.mtime = time.time()
tar.addfile(tarinfo, BytesIO(file_data))
tar.close()

#%% Put tar archive in container

client = docker.from_env()

spark_master = client.containers.get("spark-master")
spark_worker_1 = client.containers.get("spark-worker-1")
spark_worker_2 = client.containers.get("spark-worker-2")

tarstream.seek(0)
spark_master.put_archive("/opt/spark-data", tarstream) #will put data in all containers due to simulated shared storage
