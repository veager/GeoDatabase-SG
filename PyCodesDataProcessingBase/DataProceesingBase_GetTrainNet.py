import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point



def ReadTrianStaLocShp(path):
    '''
    get the train station location from the shapfile file 
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
    # read the raw shapefile
    data = gpd.read_file(path)
    data.rename(columns={'STN_NAME': 'stn_n', 'STN_NO':'stn_c_li'}, inplace=True)
    data['stn_n'] = data['stn_n'].str.strip()
    data['stn_c_li'] = data['stn_c_li'].str.strip()
    
    # rename station name
    def RenameStation(s):
        if s.endswith(' MRT STATION') or s.endswith(' LRT STATION'):
            s = s[:-len(' MRT STATION')].strip()
        s = s.strip()
        s = string.capwords(s)
        return  s
    # - - - - - - - - - - - - - - - - 
    data['stn_n'] =  data['stn_n'].map(RenameStation)
    
    data = data[['stn_n', 'geometry']]
    
    # delete duplicated
    data = data.dissolve(by=['stn_n'], as_index=False)
    data['geometry'] = data['geometry'].centroid
    data['id'] = [str(i).zfill(5) for i in range(1, data.shape[0]+1)]
    
    return data
# ----------------------------------------------------------------------------
def ReadTrainRouteCsv(path):
    '''
    get the train stop sequence (train route) from the csv file 
        the data is collected from LTA train map: https://www.lta.gov.sg/content/ltagov/en/map/train.html

    Parameters
    ----------
    path : str
        DESCRIPTION.

    Returns
    -------
    data : pandas.DataFrame
        DESCRIPTION.

    '''
    data = pd.read_csv(path)
    
    # correct some error from raw data
    # - modify the code 'DT1-BP6' of Bukit Panjang in Bukit Panjang LRT Line
    ix = (data['lne_n'] == 'Bukit Panjang LRT Line') & (data['stn_n'] == 'Bukit Panjang')
    data.loc[ix, 'stn_c'] = 'BP6'
    # - Rafles Place station is typo
    ix = data[data['stn_n'] == 'Rafles Place'].index
    data.loc[ix, 'stn_n'] = 'Raffles Place'
    # - drop manipulation is last
    # - Teck Lee station is not service
    ix = data[data['stn_n'] == 'Teck Lee'].index
    data.drop(ix, axis=0, inplace=True)
    
    data['stn_n'] = data['stn_n'].map(string.capwords)
    
    data['sublne_c'] = data['stn_c'].map(lambda x: re.sub('\d+', '', x))
    data['sublne_seq'] = data['stn_c'].map(lambda x: re.sub('^[a-zA-Z]+', '', x))   
    data.loc[data['sublne_seq']=='', 'sublne_seq'] = 1
    data['sublne_seq'] = data['sublne_seq'].map(int)
    
    data.sort_values(by=['lne_n', 'sublne_c', 'sublne_seq'], inplace=True)
    data.reset_index(drop=True, inplace=True)
    return data
# ----------------------------------------------------------------------------
def TrainRoute2Net(route):
    '''
    According to train route table, construct the train networks

    Parameters
    ----------
    route : pandas.DataFrame
        DESCRIPTION.

    Returns
    -------
    net : networkx.DiGraph
        Directed bus networks

    '''
    # train network is definited as an undirected graph
    net = nx.Graph()
    
    
    return net
# ----------------------------------------------------------------------------
# =============================================================================