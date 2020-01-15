#%% Import require packages
import h5py
from argparse import ArgumentParser
import matplotlib.pyplot as plt
import os
import dask.dataframe as dd

parser = ArgumentParser("create dynspectrum plot from processed data");
parser.add_argument('-r','--rawdata', nargs='+',help='rawdata file',dest="rawfile",required=True)
parser.add_argument('-p','--processedata', nargs='+',help='processed data folder without trailing frontslash',dest="processedfolder",required=True)
parser.add_argument('-m','--processedmedian', nargs='+',help='processed median folder without trailing frontslash',dest="medianfolder",required=True)
parser.add_argument('-i','--vmin', help='vmin of plot',dest='vmin',type=float,default=0.5)
parser.add_argument('-a','--vmax', help='vmax of plot',dest='vmax',type=float,default=2)
parser.add_argument('-c','--cmap', help='matplotlib colormap',dest='cmap',default="viridis")
parser.add_argument('-n','--show_normalization', help='plot normalization',dest='show_normalization', action='store_true')
parser.add_argument('-r','--max_res', help='maximum resolution',dest='max_res', default=2000)
parser.add_argument('-w','--wait_time', help='wait time',dest='wait_time', default=5)

def get_metadata_from_h5(h5file):
     #metadata=h5file.attrs[u'NOF_SUB_ARRAY_POINTINGS'] 
     metadata=dict(h5file[h5file.visit(lambda x: x if 'STOKES' in x else None)].attrs)
     metadata['freqs'] = h5file[h5file.visit(lambda x: x if 'COORDINATE_1' in x else None)].attrs[u'AXIS_VALUES_WORLD']
     metadata=dict(metadata,**dict(h5file[h5file.visit(lambda x: x if 'BEAM' in x else None)].attrs))
     metadata["starttime"]= h5file.attrs[u'OBSERVATION_START_MJD']
     metadata["endtime"]= h5file.attrs[u'OBSERVATION_END_MJD']
     return metadata

def plot_real_time(fig,axarr,rawfile,nch,nSB,freqs,vmin,vmax,maxSamples=10000,skiptime=25,skipch=1,skipSamples=0,starttime=None,endtime=None,sampleSize=1./125.,cmap='Reds',show_norm=False):
    ax=axarr
    if show_norm:
        ax=ax[0]
    ax.cla()
    myextent=[0,data.shape[0],freqs[0]*1e-6,freqs[::skipch][-1]*1e-6]
    if not (starttime is None):
        myextent[0]=mdates.date2num(starttime_dt)
        myextent[1]=mdates.date2num(dt.strptime(pt.taql('select str(mjdtodate({})) as date'.format(starttime+(data.shape[0]*skiptime*sampleSize)/(24.*3600.))).getcol('date')[0], '%Y/%m/%d/%H:%M:%S'))  # thanks to TJD
        ax.imshow((data/mymedian).T,origin='lower',interpolation='nearest',aspect='auto',vmin=vmin,vmax=vmax,extent=myextent,cmap=cmap)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.set_ylabel("freq (MHz)")
    if show_norm:
        ax=axarr[1]
        ax.cla()
        ax.plot(freqs[::skipch]*1e-6,mymedian,'k')
        ax.set_xlabel("freq (MHz)")
    plt.pause(.3)
    
    
    
def main(argv):
    #args=parser.parse_args(argv)
    #metadata = get_metadata_from_h5(h5py.File(args.rawfile[0].replace('.raw','.h5')))
    
    #processed_folder = args.processedfolder
    #median_folder = args.medianfolder
    
    processed_folder = "/mnt/spark-results/median_scaled_data"
    median_folder = "/mnt/spark-results/median_scaled_data"
    
    #Initalize list of filenames 
    scaled_data_filenames = []
    median_data_filenames = []
    
    all_processed_data = None
    all_median_data = None
    while(True):
        
        all_scaled_data_filenames = os.listdir(processed_folder)
        all_median_data_filenames = os.listdir(median_folder)
        
        new_scaled_data_filenames = list(set(all_scaled_data_filenames) - set(scaled_data_filenames))
        new_median_data_filenames = list(set(all_median_data_filenames) - set(median_data_filenames))
        
        #if new processed data is present
        if(len(new_scaled_data_filenames) > 0):
            
            new_processed_data = dd.read_csv([processed_folder+"/"+filename for filename in new_scaled_data_filenames])
            
            if(all_processed_data is not None):
                processed_data_list = [all_processed_data, new_processed_data]
            else:
                processed_data_list = [new_processed_data]
                
            all_processed_data = dd.concat(processed_data_list, axis=0)
        
        #if new median data is present    
        if(len(new_median_data_filenames) > 0):
        
            new_median_data = dd.read_csv([median_folder+"/"+filename for filename in new_median_data_filenames])
        
            if(all_median_data is not None):
                median_data_list = [all_median_data, new_median_data]
            else:
                median_data_list = [new_median_data]
            
            all_median_data = dd.concat(median_data_list, axis=0)
        
        #tests:
        print(all_processed_data.shape)
        print(all_median_data.shape)
        
        scaled_data_filenames = all_scaled_data_filenames
        median_data_filenames = all_median_data_filenames
        
        #time.sleep(args.wait_time)
        time.sleep(5)
        
        #check if new data is written
        #if new files:
        #  subsample in order to not get higher than max_res
        #  plot image of concatenated datasets
        #  Refresh webpage with image

    fig,axarr=plt.subplots(1+args.show_normalization,1)
    #plot_real_time(fig,axarr,rawfile,metadata['CHANNELS_PER_SUBBAND'],metadata[u'NOF_SUBBANDS'],metadata['freqs'],args.vmin,args.vmax,skipSamples=skipSamples,starttime=metadata['starttime'],endtime=metadata['endtime'],sampleSize=metadata[u'SAMPLING_TIME'],cmap=args.cmap,show_norm=args.show_normalization)

if __name__ == '__main__':
    main(sys.argv[1:])    
