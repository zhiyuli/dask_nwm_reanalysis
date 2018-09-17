import xarray as xr
import dask.array as da
from dask.diagnostics import ProgressBar
import os
import numpy as np
import time

year_list = ['2016']
#month_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
month_list = ['01']

for year in year_list:
    for month in month_list:
        # Starting timer
        start_time = time.time()
        print('{}-{}'.format(year, month))

        workdir = '/mnt/homes/NWM/noaa_reanalysis/{0}/{0}{1}'.format(year, month)

        files = [os.path.join(workdir, i) for i in os.listdir(workdir) if (".CHRTOUT_DOMAIN1.comp.nc" in i)]
        files.sort()

        # Creating the dask array for computing
        big_array = []

        for i, file in enumerate(files):
            ds = xr.open_dataset(file, chunks={"feature_id": 682270})
            big_array.append(ds.streamflow.data)
            ds.close()

        dask_big_array = da.stack(big_array, axis=0)
        mean_array_list = []

        print(dask_big_array)

        indices = np.arange(dask_big_array.shape[0]).reshape((-1, 24))

        for i in range(indices.shape[0]):
            mean_array_list.append(np.mean(dask_big_array[indices[i, :], :], axis=0))

        res = da.stack(mean_array_list)

        with ProgressBar():
            da.to_hdf5('./noaa_streamflow_daily.hdf5', '{}-{}'.format(year, month), res)

        # Printing total execution time
        print("--- {}s seconds ---".format(time.time() - start_time))
