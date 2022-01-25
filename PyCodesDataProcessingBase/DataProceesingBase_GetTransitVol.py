import pandas as pd
import geopandas as gpd



# --------------------------------------------------------------
def GetStopVol(path):
    '''

    Parameters
    ----------
    path : str
        DESCRIPTION.

    Returns
    -------
    vol : pandas.DataFrame
        DESCRIPTION.

    '''
    data = pd.read_csv(path, index_col=None, 
        dtype={'PT_CODE': str, 'TIME_PER_HOUR':str}
    )
    # BusStopCode column
    data.rename(columns={'PT_CODE': 'BusStopCode'}, inplace=True)
    
    # simplify the categorical value of DAY_TYPE column
    data.loc[data['DAY_TYPE'] == 'WEEKDAY', 'DAY_TYPE'] = 'WD'
    data.loc[data['DAY_TYPE'] == 'WEEKENDS/HOLIDAY', 'DAY_TYPE'] = 'WE'
    
    # hour list
    hour_li = data['TIME_PER_HOUR'].unique().tolist()
    
    # tap in volume 
    vol_in = data.pivot_table(
        values = 'TOTAL_TAP_IN_VOLUME',
        index = 'BusStopCode',
        columns = ['DAY_TYPE', 'TIME_PER_HOUR'],
        aggfunc = 'sum'
    )
    columns = vol_in.columns.to_flat_index().to_list()
    columns = list(map(lambda x: 'IN_' + x[0] + '_' + x[1], columns))
    vol_in.columns = columns
    
    # tap out volume 
    vol_out = data.pivot_table(
        values = 'TOTAL_TAP_OUT_VOLUME',
        index = 'BusStopCode',
        columns = ['DAY_TYPE', 'TIME_PER_HOUR'],
        aggfunc = 'sum'
    )
    columns = vol_out.columns.to_flat_index().to_list()
    columns = list(map(lambda x: 'OUT_' + x[0] + '_' + x[1], columns))
    vol_out.columns = columns
                    
    vol = vol_in.merge(
        right=vol_out, how='outer', 
        left_index=True, right_index=True
    )
    
    # total volume
    for s in ['IN', 'OUT']:
        for w in ['WD', 'WE']:
            cols = ['{0}_{1}_{2}'.format(s, w, i) for i in hour_li]
            vol['{0}_{1}_total'.format(s, w)] = vol[cols].sum(axis=1, skipna=True)
    
    vol['BusStopCode'] = vol.index.to_list()
    vol.reset_index(drop=True, inplace=True)
    # sort by BusStopCode
    vol.sort_values(by='BusStopCode', ignore_index=True, inplace=True)
    # drop duplicate BusStopCode
    vol.drop_duplicates(subset='BusStopCode', keep='first', 
        inplace=True, ignore_index=True
    )
    return vol
# --------------------------------------------------------------
def MergeStopVolLoc(data_vol, data_loc, merge_how):
    '''
    

    Parameters
    ----------
    data_vol : pandas.DataFrame
        DESCRIPTION.
    data_loc : geopandas.GeoDataFrame
        DESCRIPTION.
    merge_how : str of {'inner', 'outer', 'left', 'right'}
        DESCRIPTION. The default is False.

    Returns
    -------
    data_vol : geopandas.GeoDataFrame
        DESCRIPTION.

    '''
    data_vol = data_vol.merge(right=data_loc, on='BusStopCode',
        how=merge_how
    )  
    return data_vol
# --------------------------------------------------------------
def GetODTrip(path):
    '''
    
    Parameters
    ----------
    path : str
        DESCRIPTION.
    
    Returns
    ----------
    data : pandas.DataFrame
        DESCRIPTION.
    '''
    data = pd.read_csv(path, dtype={
        'YEAR_MONTH': str,    'TIME_PER_HOUR': str,
        'ORIGIN_PT_CODE': str, 'DESTINATION_PT_CODE': str,
    })
    
    data.rename(
        columns={'ORIGIN_PT_CODE': 'O_BusStopCode', 'DESTINATION_PT_CODE': 'D_BusStopCode'}, 
        inplace=True
    )
    # print(data.columns)
    # redundancy info
    # print('YEAR_MONTH:', data['YEAR_MONTH'].unique().tolist())
    # print('PT_TYPE:', data['PT_TYPE'].unique().tolist())
    
    data.drop(['YEAR_MONTH', 'PT_TYPE'] , axis=1, inplace=True)
    
    # change the value of DAY_TYPE column
    data.loc[data['DAY_TYPE'] == 'WEEKDAY', 'DAY_TYPE'] = 'WD'
    data.loc[data['DAY_TYPE'] == 'WEEKENDS/HOLIDAY', 'DAY_TYPE'] = 'WE'
    
    # hour list
    hour_li = data['TIME_PER_HOUR'].unique().tolist()
    
    data = data.pivot_table(
        values = 'TOTAL_TRIPS',
        index = ['O_BusStopCode', 'D_BusStopCode'],
        columns = ['DAY_TYPE', 'TIME_PER_HOUR']
    )
    
    # rename columns
    cols = data.columns.to_flat_index().to_list()
    cols = list(sorted(cols, key=lambda x: int(x[1])))
    cols = list(map(lambda x: x[0] + '_' + x[1], cols))
    data.columns = cols
    
    # total volume
    for w in ['WD', 'WE']:
        cols = ['{0}_{1}'.format(w, i) for i in hour_li]
        data['{0}_total'.format(w)] = data[cols].sum(axis=1, skipna=True)
    
    # multi-index to columns
    data.reset_index(inplace=True)
    # sort by OD
    data.sort_values(by=['O_BusStopCode', 'D_BusStopCode'], inplace=True, ignore_index=True)
    
    return data
# --------------------------------------------------------------
def ODTrip2Shp(odtrip, bus_stop_loc):
    '''
    
    '''
    crs = bus_stop_loc.crs
    bus_stop_loc = bus_stop_loc[['BusStopCode', 'geometry']]
    
    # find the start stop location
    odtrip = odtrip.merge(right=bus_stop_loc,
        how='left', left_on='O_BusStopCode', right_on='BusStopCode')
    odtrip.drop('BusStopCode', axis=1, inplace=True)
    odtrip.rename(columns={'geometry':'start'}, inplace=True)
    
    # find the end stop location
    odtrip = odtrip.merge(right=bus_stop_loc,
        how='left', left_on='D_BusStopCode', right_on='BusStopCode')
    odtrip.drop('BusStopCode', axis=1, inplace=True)
    odtrip.rename(columns={'geometry':'end'}, inplace=True)
    
    # add trip line 
    start_li = odtrip['start'].to_list()
    end_li = odtrip['end'].to_list()
    geometry = []
    from shapely.geometry import LineString
    for start, end in zip(start_li, end_li):
        try:
            geometry.append(LineString([start, end]))
        except:
            geometry.append('')
    # 
    odtrip['geometry'] = geometry
    odtrip.drop(['start', 'end'], axis=1, inplace=True)
    # drop no geometry info sample
    odtrip = odtrip[odtrip['geometry'] != '']
    odtrip.reset_index(drop=True, inplace=True)
    # 
    odtrip = gpd.GeoDataFrame(odtrip, crs=crs)
    return odtrip
# =============================================================================
