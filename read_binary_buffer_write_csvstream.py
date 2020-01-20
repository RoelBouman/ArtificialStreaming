#! /usr/bin/env python
import sys
from io import BytesIO
from argparse import ArgumentParser
import h5py
import numpy as np
import tarfile
import docker
import time
import pandas as pd
from astropy.time import Time as astroTime
from datetime import timedelta
import pickle

parser = ArgumentParser("create csvstream from raw data");
parser.add_argument('-i','--rawdata',help='rawdata file',dest="rawfile",required=True)
parser.add_argument('-d','--docker_container',help='docker container',dest="docker_container",type=str,required=True)
parser.add_argument('-fd','--docker_folder_data',help='docker folder for beamformed data',dest="docker_folder_data",type=str,required=True)
parser.add_argument('-fm','--docker_folder_metadata',help='docker folder for metadata',dest="docker_folder_metadata",type=str,required=True)
parser.add_argument('-k','--skip_samples', help='number of samples to skip at the beginning',dest='skip_samples',type=int,default=0)
parser.add_argument('-t','--skip_time', help='time sampling ratio (e.g. 25 for taking 1 in 25 samples, samples over all read samples)',dest='skip_time',type=int,default=25)
parser.add_argument('-m','--max_samples', help='number of samples to read per streaming operation, must be higher than skip_time',dest='max_samples',type=int,default=10000)
parser.add_argument('-w','--wait_time', help='time in seconds to wait between read/stream operations',dest='wait_time',type=int,default=0)

bytes_per_sample=4

def write_file_to_container(docker_container,docker_folder,data_string,file_name):

     tarstream=BytesIO()
     tar=tarfile.TarFile(fileobj=tarstream, mode='w')
     tarinfo=tarfile.TarInfo(name=file_name)
     tarinfo.size=len(data_string)
     tarinfo.mtime=time.time()
     tar.addfile(tarinfo, BytesIO(data_string))
     tar.close()

     tarstream.seek(0)
     docker_container.put_archive(docker_folder, tarstream)
     
def write_metadata(docker_container,docker_folder,metadata,file_name):
     pickled_metadata = pickle.dumps(metadata) #pickles metadata as string
     
     write_file_to_container(docker_container,docker_folder,pickled_metadata,file_name)

def write_beamformed(docker_container,docker_folder,data_part_df,starttime,endtime):
     data_string = data_part_df.to_csv(sep=",", date_format="%Y-%m-%d %H:%M:%S", index=False)
     data_string=data_string.encode('utf8')
     file_name="measurement"+str(np.round(starttime, 4))+"-"+str(np.round(endtime,4))+".csv"

     write_file_to_container(docker_container,docker_folder,data_string,file_name)

def get_metadata_from_h5(h5file):
     metadata=dict(h5file[h5file.visit(lambda x: x if 'STOKES' in x else None)].attrs)
     metadata['freqs'] = h5file[h5file.visit(lambda x: x if 'COORDINATE_1' in x else None)].attrs[u'AXIS_VALUES_WORLD']
     metadata=dict(metadata,**dict(h5file[h5file.visit(lambda x: x if 'BEAM' in x else None)].attrs))
     metadata["starttime"]= h5file.attrs[u'OBSERVATION_START_MJD']
     metadata["endtime"]= h5file.attrs[u'OBSERVATION_END_MJD']
     return metadata

def stream_real_time(docker_container,docker_folder_data,rawfile,nch,nSB,freqs,starttime,endtime,maxSamples=10000,skiptime=25,skipch=1,skipSamples=0,sampleSize=1./125.,waitTime=0):

     rawfile.seek(skipSamples*bytes_per_sample*nch*nSB)
     tSam = int(skipSamples)
     while(True):   
        mybuffer=rawfile.read(maxSamples*bytes_per_sample*nch*nSB)
        tmpdata=np.frombuffer(mybuffer,dtype=np.float32) #np.float = np.float64!!
        nSam=int(tmpdata.shape[0]/(nch*nSB))
        tSam+=nSam

        data_part_df=pd.DataFrame(tmpdata.reshape((int(nSam),(nch*nSB)))[::skiptime,::skipch])
        
        data_part_df["seconds_from_start"]=[d*sampleSize for d in range(int(tSam-nSam), int(tSam))][::skiptime]
        data_part_df["timestamp"]=[astroTime(starttime,format="mjd").datetime+timedelta(seconds=dt) for dt in data_part_df["seconds_from_start"]]
        data_part_df["hourly_timestamp"]=[time.replace(minute=0,second=0, microsecond=0) for time in  data_part_df["timestamp"]]

        write_beamformed(docker_container,docker_folder_data,data_part_df,(tSam-nSam)*sampleSize,(tSam*sampleSize))
        
        print(data_part_df.shape)
        print("streamed data from " + str(data_part_df["timestamp"].iloc[0]) + " to " + str(data_part_df["timestamp"].iloc[-1]) )
        
        time.sleep(waitTime)


def main(argv):

    args=parser.parse_args(argv)
    client=docker.from_env()
    docker_container=client.containers.get(args.docker_container)
    rawfile=open(args.rawfile,'rb')
    metadata = get_metadata_from_h5(h5py.File(args.rawfile.replace('.raw','.h5')))
    
    write_metadata(docker_container,args.docker_folder_metadata,metadata,args.rawfile.replace('.raw','_metadata.pickle'))
    
    stream_real_time(docker_container,args.docker_folder_data,rawfile,metadata['CHANNELS_PER_SUBBAND'],metadata[u'NOF_SUBBANDS'],metadata['freqs'],starttime=metadata['starttime'],endtime=metadata['endtime'],skipSamples=args.skip_samples,sampleSize=metadata[u'SAMPLING_TIME'],maxSamples=args.max_samples,waitTime=args.wait_time,skiptime=args.skip_time)


if __name__ == '__main__':
    main(sys.argv[1:])    
