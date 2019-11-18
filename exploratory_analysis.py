# -*- coding: utf-8 -*-
"""
Created on Thu Nov  7 13:55:58 2019

@author: Roel
"""
#%% Loading data
## See https://books.google.nl/books?id=C3FyDwAAQBAJ&pg=PA238&lpg=PA238&dq=.h5+.raw&source=bl&ots=D1CgyXYZjC&sig=ACfU3U1RIifIb8Rnn1pDRTalNVxBE0iebg&hl=nl&sa=X&ved=2ahUKEwjB6cSBntjlAhUPalAKHc8nAVUQ6AEwAnoECAkQAQ#v=onepage&q=.h5%20.raw&f=false
## For specs on how data is saved.

## https://github.com/cbassa/lofar_bf_tutorials/blob/master/solutions/reading_hdf5_headers.ipynb
## https://github.com/cbassa/lofar_bf_tutorials/blob/master/solutions/reading_hdf5_stokes_data.ipynb
## https://www.astron.nl/lofarschool2018/Documents/Thursday/bassa.pdf

## https://stackoverflow.com/questions/28170623/how-to-read-hdf5-files-in-python
import h5py
import numpy as np
import matplotlib.pyplot as plt

filename = 'L701913_SAP000_B000_S0_P000_bf.h5'

h5 = h5py.File(filename, "r")
## https://stackoverflow.com/questions/46733052/read-hdf5-file-into-numpy-array

#%% Showing essential attributes and structure of the 
def print_name(name):
    print(name)
    
h5.visit(print_name)

group = h5["/"]
keys = sorted(["%s"%item for item in sorted(list(group.attrs))])
for key in keys:
    print(key + " = " + str(group.attrs[key]))
    
    
group = h5["/SUB_ARRAY_POINTING_000/BEAM_000/STOKES_0"]
keys = sorted(["%s"%item for item in sorted(list(group.attrs))])
for key in keys:
    print(key + " = " + str(group.attrs[key]))
#%% plotting (part of) the data
stokes = h5["/SUB_ARRAY_POINTING_000/BEAM_000/STOKES_0"]
data = 10.0*np.log10(stokes[1::300,:])

freq = h5["/SUB_ARRAY_POINTING_000/BEAM_000/COORDINATES/COORDINATE_1"].attrs["AXIS_VALUES_WORLD"]*1e-6
tsamp = h5["/SUB_ARRAY_POINTING_000/BEAM_000/COORDINATES/COORDINATE_0"].attrs["INCREMENT"]
t = tsamp*np.arange(stokes.shape[0])

vmin = np.median(data)-2.0*np.std(data)
vmax = np.median(data)+6.0*np.std(data)

plt.figure(figsize=(20, 10))
plt.imshow(data.T, aspect='auto', vmin=vmin, vmax=vmax, origin='lower', extent=[t[0], t[-1], freq[0], freq[-1]])
plt.xlabel("Time (s)")
plt.ylabel("Frequency (MHz)")
plt.colorbar().set_label('Power (dB)', rotation=270)