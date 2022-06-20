import os
import io
import zipfile
import pandas as pd
import geopandas as gpd


'''
zip_path = 'master-plan-2019-subzone-boundary-no-sea.zip'

z = zipfile.ZipFile(zip_path)
# obtain the data path in the zip file
file_name = list(filter(lambda x: x.endswith('.kml'), z.namelist()))[0]

with z.open(file_name, 'r') as f:
    content = f.read()
    content = content.decode("utf-8")
    # 
z.close()
'''


def gpd_parse_descrp(data_gpd):
    '''
    Parse the "Decription" column by html

    Parameters
    ----------
    data_gpd : geopandas.DataFrame
        DESCRIPTION.

    Returns
    -------
    data_gpd : geopandas.DataFrame
        DESCRIPTION.

    '''
    # the number of smaple
    n = data_gpd.shape[0]
    # the added columns' names
    columns = pd.read_html(
        data_gpd['Description'][0], 
        index_col = 0)[0].index.to_list()
    
    data_gpd[columns] = ''
    
    # add data
    for i in range(n):
        data = pd.read_html(data_gpd['Description'][i], index_col=0)[0]
        data = data.squeeze()
        data_gpd.loc[i, columns] = data
    
    # drop the 'Description' column
    data_gpd = data_gpd.drop('Description', axis=1)
    return data_gpd





gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'


# - - - - - - - - - - - - - - - 
# - - - subzone
# - - - - - - - - - - - - - - - 
subzone_path = "zip://master-plan-2019-subzone-boundary-no-sea.zip!master-plan-2019-subzone-boundary-no-sea-kml.kml"
subzone_gpd = gpd.read_file(subzone_path, driver='KML')   # Read XML data
print(subzone_gpd.crs)

subzone_gpd = gpd_parse_descrp(subzone_gpd)
# Save 
subzone_gpd.to_file('shapefiles/MP2019-subzone/MP2019-subzone.shp')


# - - - - - - - - - - - - - - - 
# - - - planning area: aggregate by subzone
# - - - - - - - - - - - - - - - 
plarea_gpd = subzone_gpd.dissolve(by='PLN_AREA_C', as_index=False, aggfunc='first')
plarea_gpd = plarea_gpd.drop(
    ['Name', 'SUBZONE_NO', 'SUBZONE_N', 'SUBZONE_C', 'INC_CRC', 'FMEL_UPD_D'],
    axis=1)
# Save 
plarea_gpd.to_file('shapefiles/MP2019-planning-area/MP2019-planning-area.shp')


# - - - - - - - - - - - - - - - 
# - - - region
# - - - - - - - - - - - - - - - 
region_path = "zip://master-plan-2019-region-boundary-no-sea.zip!master-plan-2019-region-boundary-no-sea-kml.kml"
region_gpd = gpd.read_file(region_path, driver='KML')   # Read XML data
region_gpd = gpd_parse_descrp(region_gpd)
# Save 
region_gpd.to_file('shapefiles/MP2019-region/MP2019-region.shp')


# - - - - - - - - - - - - - - - 
# - - - conutry boundary: aggregate by region
# - - - - - - - - - - - - - - - 
# conutry boundary
boundary_gpd = region_gpd.dissolve(as_index=True, aggfunc='first')
boundary_gpd = boundary_gpd[['geometry']]
# Save 
boundary_gpd.to_file('shapefiles/MP2019-boundary/MP2019-boundary.shp')


# - - - - - - - - - - - - - - - 
# - - - planning area with sea
# - - - - - - - - - - - - - - - 
plarea_path = "zip://master-plan-2019-planning-area-boundary-no-sea.zip!planning-boundary-area.kml"
plarea_gpd = gpd.read_file(plarea_path, driver='KML')   # Read XML data
plarea_gpd = gpd_parse_descrp(plarea_gpd)
# Save 
plarea_gpd.to_file('shapefiles/MP2019-planning-area-with-sea/MP2019-planning-area-with-sea.shp')


# - - - - - - - - - - - - - - - 
# - - - conutry boundary with sea: aggregate by planning area with sea
# - - - - - - - - - - - - - - - 
boundary_gpd = plarea_gpd.dissolve(as_index=True, aggfunc='first')
boundary_gpd = boundary_gpd[['geometry']]
# Save 
boundary_gpd.to_file('shapefiles/MP2019-boundary-with-sea/MP2019-boundary-with-sea.shp')



