# Calculate the average for a specific day over multiple years

import dask.array as da
from dask.diagnostics import ProgressBar
import h5py
import pandas as pd
import numpy as np

total_date_range = pd.date_range('1993-01-01', '2017-12-31').strftime('%m-%d')
one_leap_year_dates = pd.date_range('2000-01-01', '2000-12-31').strftime('%m-%d')

indices = [np.where(i == total_date_range) for i in one_leap_year_dates]

f = h5py.File('./noaa_streamflow_daily.hdf5')

big_array_list = []

for key in f.keys():
    dset = f[key]
    big_array_list.append(da.from_array(dset, chunks=(31, 25000)))

big_dask_array = da.concatenate(big_array_list)

daily_avg_list = []

for indice in indices:
    daily_avg_list.append(np.mean(big_dask_array[indice[0], :], axis=0))

daily_avg_array_dask = da.stack(daily_avg_list)

with ProgressBar():
    da.to_hdf5(
        './noaa_streamflow_daily_statistics.hdf5', 'Daily Averages',
        daily_avg_array_dask
    )
