#%% Import require packages
import h5py
from argparse import ArgumentParser
import matplotlib.pyplot as plt
import os
import pandas as pd
import matplotlib.dates as mdates
import time
import sys
from datetime import datetime as dt
from dateutil.parser import parse
import re

parser = ArgumentParser("create dynspectrum plot from processed data");
parser.add_argument('-r','--rawdata', nargs='+',help='rawdata file',dest="rawfile",required=True)
#parser.add_argument('-p','--processedata', nargs='+',help='processed data folder without trailing frontslash',dest="processedfolder",required=True)
#parser.add_argument('-m','--processedmedian', nargs='+',help='processed median folder without trailing frontslash',dest="medianfolder",required=True)
parser.add_argument('-i','--vmin', help='vmin of plot',dest='vmin',type=float,default=0.5)
parser.add_argument('-a','--vmax', help='vmax of plot',dest='vmax',type=float,default=2)
parser.add_argument('-c','--cmap', help='matplotlib colormap',dest='cmap',default="viridis")
parser.add_argument('-n','--show_normalization', help='plot normalization',dest='show_normalization', action='store_true')
parser.add_argument('-w','--wait_time', help='wait time',dest='wait_time', default=5)
parser.add_argument('-p','--show_power_filter', help='plot power filter',dest='show_power_filter', action='store_true')
parser.add_argument('-t','--threshold', help='power filter threshold',dest='threshold', default=2e+17)


#https://stackoverflow.com/questions/5967500/how-to-correctly-sort-a-string-with-a-number-inside
def atof(text):
    try:
        retval = float(text)
    except ValueError:
        retval = text
    return retval

#https://stackoverflow.com/questions/5967500/how-to-correctly-sort-a-string-with-a-number-inside
def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    float regex comes from https://stackoverflow.com/a/12643073/190597
    '''
    return [ atof(c) for c in re.split(r'[+-]?([0-9]+(?:[.][0-9]*)?|[.][0-9]+)', text) ]

def get_metadata_from_h5(h5file):
     #metadata=h5file.attrs[u'NOF_SUB_ARRAY_POINTINGS'] 
     metadata=dict(h5file[h5file.visit(lambda x: x if 'STOKES' in x else None)].attrs)
     metadata['freqs'] = h5file[h5file.visit(lambda x: x if 'COORDINATE_1' in x else None)].attrs[u'AXIS_VALUES_WORLD']
     metadata=dict(metadata,**dict(h5file[h5file.visit(lambda x: x if 'BEAM' in x else None)].attrs))
     return metadata

def plot_real_time(fig,axarr,processed_data,freqs,vmin,vmax,median_data,maxSamples=10000,skiptime=25,skipch=1,sampleSize=1./125.,cmap='Reds',show_norm=False, show_power_filter=False, threshold=2e+17):
    ax=axarr
    
    starttime_dt = parse(processed_data.iloc[0,-1])
    endtime_dt = parse(processed_data.iloc[-1,-1])
    
    fig.suptitle(starttime_dt.strftime("%m/%d/%Y"))
     
    if show_norm:
        ax=ax[0]
    ax.cla()
    myextent=[0,processed_data.shape[0],freqs[0]*1e-6,freqs[::skipch][-1]*1e-6]
    myextent[0]=mdates.date2num(starttime_dt)
    myextent[1]=mdates.date2num(endtime_dt)
    
    if show_power_filter:
        skip_cols = -2
    else:
        skip_cols = -1
    
    ax.imshow((processed_data.iloc[:,:skip_cols]).T,origin='lower',interpolation='nearest',aspect='auto',vmin=vmin,vmax=vmax,extent=myextent,cmap=cmap)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.set_ylabel("freq (MHz)")
    if show_power_filter:
        ax=axarr[1]
        ax.cla()
        float_dates = [mdates.date2num(parse(date)) for date in processed_data.iloc[:,-1]]
        ax.plot(float_dates,processed_data.iloc[:,-2],'k') 
        ax.axhline(y=threshold,color='r',linestyle='-')
        ax.set_ylabel("Sum of Squares")
        ax.set_xlim(mdates.date2num(starttime_dt), mdates.date2num(endtime_dt))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    if show_norm:
        
        if show_power_filter:
            ax=axarr[2]
        else:
            ax=axarr[1]
        ax.cla()
        ax.plot(freqs[::skipch]*1e-6,median_data.iloc[-1,:],'k') #plot only last acquired median
        ax.set_xlabel("freq (MHz)")

    plt.pause(.3)
    
    
    
def main(argv):
    args=parser.parse_args(argv)
    metadata = get_metadata_from_h5(h5py.File(args.rawfile[0].replace('.raw','.h5')))
    
    #processed_folder = args.processedfolder
    #median_folder = args.medianfolder
    
    processed_folder = "/mnt/spark-results/median_scaled_data"
    median_folder = "/mnt/spark-results/medians"
    
    #Initalize list of filenames 
    scaled_data_filenames = []
    median_data_filenames = []
    
    all_processed_data = None
    all_median_data = None
    
    fig,axarr=plt.subplots(1+args.show_normalization+args.show_power_filter,1)
    while(True):
        
        all_scaled_data_filenames = os.listdir(processed_folder)
        all_median_data_filenames = os.listdir(median_folder)
        
        new_scaled_data_filenames = sorted(list(set(all_scaled_data_filenames) - set(scaled_data_filenames)), key=natural_keys)
        new_median_data_filenames = sorted(list(set(all_median_data_filenames) - set(median_data_filenames)), key=natural_keys)
        
        scaled_data_filenames = all_scaled_data_filenames
        median_data_filenames = all_median_data_filenames
        
        #if new processed data is present
        if(len(new_scaled_data_filenames) > 0):
            
            processed_filepaths = [processed_folder+"/"+filename for filename in new_scaled_data_filenames]
            new_processed_data = pd.concat(map(pd.read_csv, processed_filepaths), axis=0)
            
            if(all_processed_data is not None):
                processed_data_list = [all_processed_data, new_processed_data]
            else:
                processed_data_list = [new_processed_data]
                
            all_processed_data = pd.concat(processed_data_list, axis=0)
            
        #if new median data is present    
        #Procedure is not needed for real-time plotting, as only the latest median is of interest. Still implemented for possible interactive plotting functionality later on.
        if(len(new_median_data_filenames) > 0):
        
            median_filepaths = [median_folder+"/"+filename for filename in new_median_data_filenames]
            new_median_data = pd.concat(map(pd.read_csv, median_filepaths), axis=0)
            
            if(all_median_data is not None):
                median_data_list = [all_median_data, new_median_data]
            else:
                median_data_list = [new_median_data]
            
            all_median_data = pd.concat(median_data_list, axis=0) #for nice distributed/lazy operation, do not use compute, as this loads everything into memory!
    
        if((len(new_median_data_filenames) > 0) | (len(new_scaled_data_filenames) > 0)):
            plot_real_time(fig,axarr,all_processed_data, metadata['freqs'],args.vmin,args.vmax,median_data=all_median_data,sampleSize=metadata[u'SAMPLING_TIME'],cmap=args.cmap,show_norm=args.show_normalization, show_power_filter=args.show_power_filter, threshold=args.threshold)

        
        time.sleep(args.wait_time)

if __name__ == '__main__':
    main(sys.argv[1:])    
