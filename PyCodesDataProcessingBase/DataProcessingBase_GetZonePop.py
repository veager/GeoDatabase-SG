import pandas as pd
import geopandas as gpd


def ReadZoneShp(path):
    '''
    The zone have three level:
        - region, 332
        - planning_area, 55
        - subzone, 5

    Parameters
    ----------
    path : TYPE
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    '''
    # --------------------------------------------------
    def _GeoAddDescrp(geopd):
        # the number of smaple
        n = geopd.shape[0]
        # the added columns' names
        columns = pd.read_html(
            geopd['Description'][0], 
            index_col = 0
        )[0].index.to_list()
        
        geopd[columns] = ''
        # add data
        for i in range(n):
            data = pd.read_html(geopd['Description'][i], index_col=0)[0]
            data = data.squeeze()
            geopd.loc[i, columns] = data
        # drop the 'Description' column
        geopd = geopd.drop('Description', axis=1)
        return geopd
    # --------------------------------------------------
    # import fiona
    gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'
    data = gpd.read_file(path, driver='KML')
    # print(data.crs)
    
    data = _GeoAddDescrp(data)
    data = data[[
        'SUBZONE_N', 'SUBZONE_C', 
        'PLN_AREA_N', 'PLN_AREA_C',
        'REGION_N', 'REGION_C',
        'geometry'
    ]]
    # print(data['SUBZONE_N'].unique().size)
    # print(data['PLN_AREA_N'].unique().size)
    # print(data['REGION_N'].unique().size)
    
    return data
# -----------------------------------------------------------------------------
def ReadZonePop(path):
    # read population data
    data = pd.read_csv(path, dtype={'year': str})
    
    data = data.rename(columns={
        'planning_area': 'PLN_AREA_N',
        'subzone': 'SUBZONE_N'
    })
    data['PLN_AREA_N'] = data['PLN_AREA_N'].str.upper()
    data['SUBZONE_N'] = data['SUBZONE_N'].str.upper()
    return data
# -----------------------------------------------------------------------------
def CheckPopAndZoneMatch(pop, zone):
    '''
    Check the two columns PLN_AREA_N and PLN_AREA_N's values whether are same
    '''
    pop_planarea = set(pop['PLN_AREA_N'].unique().tolist())
    zone_planarea = set(zone['PLN_AREA_N'].unique().tolist())

    pop_subzone = set(pop['SUBZONE_N'].unique().tolist())
    zone_subzone = set(zone['SUBZONE_N'].unique().tolist())
    
    planarea_same = pop_planarea == zone_planarea
    print('values of PLN_AREA_N are identical: {0}'.format(planarea_same))
    if not planarea_same:
        pass
    
    subzone_same = pop_subzone == zone_subzone
    print('values of SUBZONE_N are identical: {0}'.format(subzone_same))
    if not subzone_same:
        print('In population values:', pop_subzone.difference(zone_subzone))
        print('In Zone values:', zone_subzone.difference(pop_subzone))   
    return None
# -----------------------------------------------------------------------------
def PopExtract(pop, attrib='year', zone_level='SUBZONE_N', year=None):
    '''
    extract population according to attribution, year
    
    Parameters
    ----------
    pop : pandas.DataFrame
        DESCRIPTION.
    attrib : str of {'year', 'sex', 'age_group', 'type_of_dwelling'}
        'year' :
        'sex' :
        'age_group' :
        'type_of_dwelling' :
    zone_level : str of {'SUBZONE_N', 'PLN_AREA_N'}
        DESCRIPTION.
    year : None of str of '2011'~'2019'
        None : if column='year', ignore
        str of '2011'~'2019': the corresponding year
    
    '''
    # column index: 
    pivot_row, pivot_column = zone_level, attrib

    # # planning area, the number is 55
    # planarea_list = pop['PLN_AREA_N'].unique().tolist()
    # # subzone, the number is 332
    # subzone_list = pop['SUBZONE_N'].unique().tolist()
    # year_list = pop['year'].unique().tolist()
    # sex_list = pop['sex'].unique().tolist()
    # age_list = pop['age_group'].unique().tolist()
    # dwell_list = pop['type_of_dwelling'].unique().tolist()
    
    # columns' names
    pop_columns = pop[attrib].unique().tolist()
    # selected year 
    if not (pivot_column == 'year'):
        if year is None:
            year = '2019'
        pop = pop[pop['year'] == year]
        # print(year)
    # pivot table
    pop = pop.pivot_table(values='resident_count', aggfunc='sum',
        index = pivot_row,  columns = pivot_column, 
    )
    pop['total'] = pop.sum(axis=1)
    return pop
# -----------------------------------------------------------------------------
def ZoneAddPop(zone, pop, zone_level):
    '''
    Zone add population data
    
    Parameters
    ----------
    zone : geopandas.GeoDataFrame
        DESCRIPTION.
    pop : pandas.DataFrame
        DESCRIPTION.
        index: zone nane, values of 'SUBZONE_N', 'SUBZONE_N', or 'REGION_N'
        columns: population item
    zone_level: str of {'SUBZONE_N', 'PLN_AREA_N', 'REGION_N'}
        indicate the zone level:
            'SUBZONE_N': 332
            'PLN_AREA_N': 55
            'REGION_N': 5
    Returns
    -------
    zone : geopandas.GeoDataFrame

    '''
    # check parameter 'zone_type' if in the column of 'zone' or 'pop'
    try:
        assert (zone_level in zone.columns.to_list()) and (zone_level == pop.index.name)
    except:
        print('zone_level:', zone_level)
        print('zone.columns:', zone.columns.to_list())
        print('pop.index.name:', pop.index.name)
        raise AssertionError
        
    zone = zone.merge(right = pop, how = 'left', 
        left_on = zone_level, right_on = pop.index
    )
    if zone_level == 'SUBZONE_N':
        # the columns' names of 'pop' data
        pop_columns = pop.columns
        
        # allocation the population of 'TENGAH', 'WESTERN WATER CATCHMENT', 'LAKESIDE' from 'pop' to 'zone'
        # 'TENGAH'： population value is 0         
        #    -->  'PLN_AREA_N' column in 'zone', six values in 'SUBZONE_N' column
        # 'WESTERN WATER CATCHMENT'  
        #    -->  'PLN_AREA_N' column in 'zone', six values in 'SUBZONE_N' column
        # 'LAKESIDE'                 
        #    -->  'LAKESIDE (BUSINESS)' and 'LAKESIDE (LEISURE)'  in 'SUBZONE_N' column in 'zone'

        # ----------------
        # 'WESTERN WATER CATCHMENT'  
        #    -->  'PLN_AREA_N' column in 'zone' three subzones in total
        # population of 'WESTERN WATER CATCHMENT'
        pop_area = pop.loc['WESTERN WATER CATCHMENT', :].values
        # corresbonding index for 'zone' in 'PLN_AREA_N' column
        zone_ix = zone['PLN_AREA_N'] == 'WESTERN WATER CATCHMENT'
        zone_ix = zone_ix.values
        # print(zone_ix)
        # print(sum(zone_ix.to_list()))
        pop_subzone = pop_area / sum(zone_ix.tolist())
        pop_subzone = pop_subzone.astype(int)
        zone.loc[zone_ix, pop_columns] = pop_subzone

        # ----------------
        # 'LAKESIDE'                 
        #    -->  'LAKESIDE (BUSINESS)' and 'LAKESIDE (LEISURE)'  (in 'SUBZONE_N' column) in 'zone'
        pop_area = pop.loc['LAKESIDE', :].values
        pop_subzone = pop_area / 2.
        pop_subzone = pop_subzone.astype(int)
        zone.loc['LAKESIDE (BUSINESS)', pop_columns] = pop_subzone
        zone.loc['LAKESIDE (LEISURE)', pop_columns] = pop_subzone

        # -----------------
        # fill missing data
        zone[pop_columns] = zone[pop_columns].fillna(0)
            
    return zone
# =============================================================================
