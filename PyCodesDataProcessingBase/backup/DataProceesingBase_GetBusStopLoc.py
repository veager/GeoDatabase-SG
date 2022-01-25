import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


def ReadBusStopLocShp(path):
    '''
    get the bus stop location from the shapfile file 
        the data source from datamall Static Datasets

    Parameters
    ----------
    path : str
        DESCRIPTION.

    Returns
    -------
    data : geopandas.geoDataFrame
        DESCRIPTION.

    '''
    data = gpd.read_file(
        path, 
        dtype = {'BUS_STOP_N':str}
    )
    
    data = data.rename(
        {'BUS_STOP_N': 'BusStopCode',
         'LOC_DESC': 'Description'}, 
        axis=1
    )
    # 
    data = data[['BusStopCode', 'Description', 'geometry']]
    # drop duplicate
    data.drop_duplicates(inplace=True, ignore_index=True)
    return data
# --------------------------------------------------------------
# =============================================================================