import numpy as np
import pandas as pd
import geopandas as gpd

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
    data : geopandas.GeoDataFrame
        DESCRIPTION.

    '''
    data = gpd.read_file(path, 
        dtype = {'BUS_STOP_N':str}
    )
    
    data = data.rename( {'BUS_STOP_N': 'BusStopCode', 'LOC_DESC': 'Description'}, 
        axis=1)
    # 
    data = data[['BusStopCode', 'Description', 'geometry']]
    
    # drop duplicate
    data.drop_duplicates(subset='BusStopCode', inplace=True, ignore_index=True)
    data.sort_values(by='BusStopCode', ignore_index=True, inplace=True)
    
    return data
# ------------------------------------------------------------------------
def ReadBusStopLocCsv(path):
    '''
    get the bus stop location from the csv 
        the data source from datamall dynamic datasets (API)

    Parameters
    ----------
    path : str
        DESCRIPTION.

    Returns
    -------
    data : geopandas.GeoDataFrame
        DESCRIPTION.

    '''
    data = pd.read_csv(path, index_col=0,
        dtype = {'BusStopCode':str})
    
    data = gpd.GeoDataFrame(
        data.drop(['Latitude', 'Longitude'], axis=1), 
        geometry = gpd.points_from_xy(data['Longitude'], data['Latitude']),
        crs = 'epsg:4326')
    
    # drop duplicate
    data.drop_duplicates(subset='BusStopCode', inplace=True, ignore_index=True)
     data.sort_values(by='BusStopCode', ignore_index=True, inplace=True)
    
    return data
# ------------------------------------------------------------------------
def BusStopLocUpdate(data1, data2):
    '''
    updata bus stop location information

    Parameters
    ----------
    data1 : geopandas.GeoDataFrame
        referred columns, crs
    
    data2 : geopandas.GeoDataFrame
        DESCRIPTION.
    
    Returns
    -------
    data : geopandas.GeoDataFrame
        DESCRIPTION.

    '''
    # transform the CRS of data 2
    data2 = data2.to_crs(data1.crs)
    
    data2 = data2[data1.columns]
    
    data = pd.concat([data1, data2], axis=0, join='outer', ignore_index=True )
    
    # drop duplicate
    data.drop_duplicates(subset='BusStopCode', inplace=True, ignore_index=True)
    
    data.sort_values(by='BusStopCode', ignore_index=True, inplace=True)
    
    return data
# =============================================================================