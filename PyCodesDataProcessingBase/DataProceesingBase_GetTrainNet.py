import re
import string
import numpy as np
import pandas as pd
import geopandas as gpd
import networkx as nx

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
        return s
    # - - - - - - - - - - - - - - - - 
    data['stn_n'] =  data['stn_n'].map(RenameStation)
    
    data = data[['stn_n', 'geometry']]
    
    # delete duplicated
    data = data.dissolve(by=['stn_n'], as_index=False)
    data['geometry'] = data['geometry'].centroid
    data['id'] = [str(i).zfill(5) for i in range(1, data.shape[0]+1)]
    
    # delete unopening staion
    del_sta_li = ['Teck Lee', 'Bukit Brown', 'Hume']
    # - Teck Lee station is not service
    # - Bukit Brown, Hume are not openning
    del_sta_li = list(map(string.capwords, del_sta_li))
    ix = data['stn_n'] == del_sta_li[0]
    for sta in del_sta_li[1:]:
        ix = ix | (data['stn_n'] == sta)
    data = data[~ix]
    
    data.reset_index(drop=True, inplace=True)
    
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
    
    # capitalize the first letter of each word for station name
    data['stn_n'] = data['stn_n'].map(string.capwords)
    
    # create line code
    lne_c = {e: str(i+1).zfill(2) for i,e in enumerate(data['lne_n'].unique().tolist())}
    data['lne_c'] = data['lne_n'].map(lne_c)
    
    # obtain subline code
    data['sublne_c'] = data['stn_c'].map(lambda x: re.sub('\d+', '', x))
    # obtain subline sequence
    data['sublne_seq'] = data['stn_c'].map(lambda x: re.sub('^[a-zA-Z]+', '', x))   
    data.loc[data['sublne_seq']=='', 'sublne_seq'] = 1
    data['sublne_seq'] = data['sublne_seq'].map(int)
    
    data.sort_values(by=['lne_n', 'sublne_c', 'sublne_seq'], inplace=True)
    data.reset_index(drop=True, inplace=True)
    return data
# ----------------------------------------------------------------------------
def TrainRoute2Net(stn, route, multiedge=False):
    '''
    According to train route table, construct the train networks

    Parameters
    ----------
    stn : geopandas.GeoDataFrame
        DESCRIPTION.
        
    route : pandas.DataFrame
        DESCRIPTION.

    Returns
    -------
    net : networkx.DiGraph
        Directed bus networks

    '''
    route = route.merge(right=stn, 
        on='stn_n', how='left')
    route.sort_values(by=['lne_c', 'sublne_c', 'sublne_seq'], ignore_index=True, inplace=True)
    
    # for each edge
    subline_li = route['sublne_c'].unique().tolist()
    
    if multiedge:
        # multi undirected graph
        net = nx.MultiGraph()
        # for each subline
        for sublne_c in subline_li:
            subline_pd = route[route['sublne_c']==sublne_c].copy()
            subline_pd.sort_values(by='sublne_seq', ignore_index=True, inplace=True)
            # for each station on current line
            for i in range(subline_pd.shape[0]-1):
                start_node = subline_pd.loc[i, 'id']
                end_node = subline_pd.loc[i+1, 'id']
                lne_c = subline_pd.loc[i, 'lne_c']
                
                net.add_edge(start_node, end_node, lne_c)
                net.edges[start_node, end_node, lne_c]['lne_c'] = lne_c
                net.edges[start_node, end_node, lne_c]['lne_n'] = subline_pd.loc[i, 'lne_n']
                net.edges[start_node, end_node, lne_c]['sublne_c'] = sublne_c
    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  - 
    else:
        # train network is definited as an undirected graph
        net = nx.Graph()
        # for each subline
        for sublne_c in subline_li:
            subline_pd = route[route['sublne_c']==sublne_c].copy()
            subline_pd.sort_values(by='sublne_seq', ignore_index=True, inplace=True)
            # for each station on current line
            for i in range(subline_pd.shape[0]-1):
                start_node = subline_pd.loc[i, 'id']
                end_node = subline_pd.loc[i+1, 'id']
                
                lne_n = subline_pd.loc[i, 'lne_n']
                lne_c = subline_pd.loc[i, 'lne_c']
                
                # a new route between two stop node
                if not net.has_edge(start_node, end_node):
                    net.add_edge(start_node, end_node)
                    net.edges[start_node, end_node]['sublne_c'] = [sublne_c]
                    net.edges[start_node, end_node]['lne_c'] = [lne_c]
                    net.edges[start_node, end_node]['lne_n'] = [lne_n]
                    net.edges[start_node, end_node]['lne_num'] = 1
                # already existing route
                else:
                    sublne_li = net.edges[start_node, end_node]['sublne_c'].copy()
                    sublne_li.append(sublne_c)
                    sublne_li = list(set(sublne_li))
                    
                    net.edges[start_node, end_node]['sublne_c'] = sublne_li.copy()
                    
                    lne_c_li = net.edges[start_node, end_node]['lne_c'].copy()
                    lne_c_li.append(lne_c)
                    lne_c_li = list(set(lne_c_li))
                    
                    lne_n_li = net.edges[start_node, end_node]['lne_n'].copy()
                    lne_n_li.append(lne_n)
                    lne_n_li = list(set(lne_n_li))
                    
                    net.edges[start_node, end_node]['lne_c'] = lne_c_li.copy()
                    net.edges[start_node, end_node]['lne_n'] = lne_n_li.copy()
                    net.edges[start_node, end_node]['lne_num'] = len(lne_n_li)
    # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  - 

    # add bus stop information
    cols = ['id', 'stn_c', 'stn_n', 'sublne_c', 'lne_c', 'lne_n', 'geometry']
    route = route[cols]
    # drop duplicate
    route.drop_duplicates(subset=['id', 'stn_c', 'stn_n', 'sublne_c', 'lne_c', 'lne_n'], 
        ignore_index=True, inplace=True)
    route.sort_values(by=['id', 'stn_c', 'lne_c'], ignore_index=True, inplace=True)
    
    route = gpd.GeoDataFrame(route, crs=stn.crs)
    
    for group in route.groupby(by='id'):
        (node, group) = group
        net.nodes[node]['stn_c'] = group['stn_c'].unique().tolist()
        net.nodes[node]['stn_n'] = group['stn_n'].values[0]
        
        net.nodes[node]['lne_c'] = group['lne_c'].unique().tolist()
        net.nodes[node]['lne_n'] = group['lne_n'].unique().tolist()
        net.nodes[node]['lne_num'] = len(net.nodes[node]['lne_n'])
        
        net.nodes[node]['sublne_c'] = group['sublne_c'].unique().tolist()
        net.nodes[node]['sublne_num'] = len(net.nodes[node]['sublne_c'])
        
        net.nodes[node]['lng'] = group['geometry'].x.values[0]
        net.nodes[node]['lat'] = group['geometry'].y.values[0]
    
    return net
# ----------------------------------------------------------------------------
def TrainNetUpdataEdge(net, stn, route):
    '''
    updata the train route (edge) not exist in train route table
    '''
    route = route.merge(right=stn, 
        on='stn_n', how='left')
    route.drop_duplicates(subset=['id', 'stn_n', 'lne_c', 'lne_n', 'sublne_c'])
    route.sort_values(by=['lne_c', 'sublne_c', 'id'], ignore_index=True, inplace=True)
    
    # added edged list
    edge_li = [
        [('Expo', 'Tanah Merah'),     'CG', 'East-West Line'],
        [('Bayfront', 'Promenade'),   'CE', 'Circle Line'],
        [('Bukit Panjang', 'Senja'),  'BP', 'Bukit Panjang LRT Line'],
        [('Sengkang', 'Cheng Lim'),   'SW', 'Sengkang LRT line'],
        [('Sengkang', 'Renjong'),     'SW', 'Sengkang LRT line'],
        [('Sengkang', 'Compassvale'), 'SE', 'Sengkang LRT line'], # STC
        [('Sengkang', 'Ranggung'),    'SE', 'Sengkang LRT line'], # STC
        [('Punggol', 'Cove'),         'PE', 'Punggol LRT line'], 
        [('Punggol', 'Damai'),        'PE', 'Punggol LRT line'],  # PTC
        [('Punggol', 'Sam Kee'),      'PW', 'Punggol LRT line'],  # PTC
        [('Punggol', 'Soo Teck'),     'PW', 'Punggol LRT line']
    ]
    # capitalization the first letter of station name
    edge_li = [[(string.capwords(e[0][0]), string.capwords(e[0][1])), e[1], e[2]] for e in edge_li]
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _NetAddEdge(net, route, stn1, stn2, **kwargs):
        start_node = route[route['stn_n'] == stn1]['id'].values[0]
        end_node = route[route['stn_n'] == stn2]['id'].values[0]
        
        sublne_c = kwargs['sublne_c']
        lne_n = kwargs['lne_n']
        lne_c = route[route['lne_n'] == lne_n]['lne_c'].values[0]
        
        if isinstance(net, nx.MultiGraph):
            net.add_edge(start_node, end_node, lne_c)
            net.edges[start_node, end_node, lne_c]['lne_c'] = lne_c
            net.edges[start_node, end_node, lne_c]['lne_n'] = lne_n
            net.edges[start_node, end_node, lne_c]['sublne_c'] = sublne_c
        elif isinstance(net, nx.Graph):
            # a new route between two stop node
            if not net.has_edge(start_node, end_node):
                net.add_edge(start_node, end_node)
                net.edges[start_node, end_node]['sublne_c'] = [sublne_c]
                net.edges[start_node, end_node]['lne_c'] = [lne_c]
                net.edges[start_node, end_node]['lne_n'] = [lne_n]
                net.edges[start_node, end_node]['lne_num'] = 1
            # already existing route
            else:
                sublne_li = net.edges[start_node, end_node]['sublne_c'].copy()
                sublne_li.append(sublne_c)
                sublne_li = list(set(sublne_li))

                net.edges[start_node, end_node]['sublne_c'] = sublne_li.copy()

                lne_c_li = net.edges[start_node, end_node]['lne_c'].copy()
                lne_c_li.append(lne_c)
                lne_c_li = list(set(lne_c_li))

                lne_n_li = net.edges[start_node, end_node]['lne_n'].copy()
                lne_n_li.append(lne_n)
                lne_n_li = list(set(lne_n_li))

                net.edges[start_node, end_node]['lne_c'] = lne_c_li.copy()
                net.edges[start_node, end_node]['lne_n'] = lne_n_li.copy()
                net.edges[start_node, end_node]['lne_num'] = len(lne_n_li)
        return net
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    for [(start, end), sublne_c, lne_n] in edge_li:
        net = _NetAddEdge(net, route, stn1=start, stn2=end, sublne_c=sublne_c, lne_n=lne_n)
    
    return net
# ----------------------------------------------------------------------------------
def TrainNet2RouteShp(net):
    '''
    '''
    from shapely.geometry import LineString, Point
    
    if isinstance(net, nx.MultiGraph):
        pass
    elif isinstance(net, nx.Graph):
        edge = list(net.edges)[0]
        cols = list(net.edges[edge].keys())
        exd_cols = ['source', 'target', 'geometry']
        cols.extend(exd_cols)
        
        route = pd.DataFrame(columns=cols)
    
        row = {}
        for col in exd_cols:
            cols.remove(col)
        
        for (source, target) in net.edges:
            row['source'] = source
            row['target'] = target
            row['lne_c'] = ','.join(net.edges[source, target]['lne_c'])
            row['lne_n'] = ','.join(net.edges[source, target]['lne_n'])
            row['lne_num'] = net.edges[source, target]['lne_num']
            row['sublne_c'] = ','.join(net.edges[source, target]['sublne_c'])
            
            source_geo = Point(net.nodes[source]['lng'], net.nodes[source]['lat'])
            target_geo = Point(net.nodes[target]['lng'], net.nodes[target]['lat'])
            row['geometry'] = LineString([source_geo, target_geo])
            
            route = route.append(row, ignore_index=True)
        
        route = gpd.GeoDataFrame(route, geometry='geometry')
    # - - - - - - - - - - - - - - - - - - - - - 
    return route
# -----------------------------------------------------------------------------------
def PathGraph2Nodelist(graph):
    '''
    a path graph or a loop path graph (nx.Graph, undirected graph) to node list
    '''
    node_seq = []
    
    node_li = list(graph.nodes)
    for node in node_li:
        # find the start node
        if len(list(graph.neighbors(node))) == 1:
            sta_node = node
            cycle_graph = False
            break
    else: # cannot find a start node, it is a loop
        sta_node = node
        cycle_graph = True
    
    node_seq.append(sta_node)
    node_li.remove(sta_node)
    
    while len(node_li) > 0:
        neigh = graph.neighbors(sta_node)
        for n in neigh:
            if n in node_li:
                sta_node = n
                node_seq.append(sta_node)
                node_li.remove(sta_node)
                break # for
    
    if cycle_graph:
        node_seq.append(node_seq[0])
    
    return node_seq
# ----------------------------------------------------------------------------------------------------
def TrainNet2LineShp(net, route):
    
    assert isinstance(net, nx.MultiGraph)
    
    sublne_c_li = route['sublne_c'].unique().tolist()
    cnt = route['sublne_c'].value_counts()
    sublne_c_li = cnt[cnt > 1].index.to_list()
    
    sublin_dict = {i: nx.Graph() for i in sublne_c_li}
    
    for i, j, lin_c in net.edges:
        sublne_c = net.edges[i, j, lin_c]['sublne_c']
        sublin_dict[sublne_c].add_edge(i, j)
        sublin_dict[sublne_c].edges[i, j]['sublne_c'] = sublne_c
        sublin_dict[sublne_c].edges[i, j]['lne_c'] = lin_c
        sublin_dict[sublne_c].edges[i, j]['lne_n'] = net.edges[i, j, lin_c]['lne_n']
    
    from shapely.geometry import LineString, Point
    
    line_shp = gpd.GeoDataFrame(
        columns = ['sublne_c', 'lne_c', 'lne_n', 'stn_seq', 'geometry'])
    
    # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  
    def generate_row(sub_graph, whole_graph):
        row = {}
        edge_0 = list(sub_graph.edges)[0]
        row['sublne_c'] = sub_graph.edges[edge_0]['sublne_c']
        row['lne_c']    = sub_graph.edges[edge_0]['lne_c']
        row['lne_n']    = sub_graph.edges[edge_0]['lne_n']
        row['stn_seq']  = PathGraph2Nodelist(sub_graph)
        row['geometry'] = LineString([Point(whole_graph.nodes[n]['lng'], whole_graph.nodes[n]['lat']) for n in row['stn_seq']])
        
        row['stn_seq']  = ','.join(PathGraph2Nodelist(sub_graph))
        return row
    # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  
    for sublne_c, graph in sublin_dict.items():
        node_li = list(graph.nodes)
        if sublne_c == 'BP':
            # the cycling graph
            subgraph1 = nx.algorithms.cycles.find_cycle(graph)
            subgraph1 = graph.edge_subgraph(subgraph1)
            
            row = generate_row(subgraph1, net)
            line_shp = line_shp.append(row, ignore_index=True)
            
            subgraph2 = graph.copy()
            subgraph2.remove_edges_from(subgraph1.edges)
            subgraph2.remove_nodes_from(list(nx.isolates(subgraph2)))
            
            row = generate_row(subgraph2, net)
            line_shp = line_shp.append(row, ignore_index=True)
        else:
            row = generate_row(graph, net)
            line_shp = line_shp.append(row, ignore_index=True)
            
    return line_shp
# ------------------------------------------------------------------------------------------------
def TrainNet2NodeEges(net):
    '''
    Transfer the bus network (netwrokx.DiGraph object) to nodes and edges (pandas.DataFram)
    to input Gephi.
    
    Parameters
    ----------
    net : netwrokx.DiGraph
        bus network

    Returns
    -------
    node_pd : pandas.DataFrame
        DESCRIPTION.
    edge_pd : pandas.DataFrame
        DESCRIPTION.

    '''
    cols = ['Id', 'stn_c', 'stn_n', 'lne_c', 'lne_n', 'lne_num', 'sublne_c', 'sublne_num', 'lng', 'lat']
    node_pd = pd.DataFrame(columns = cols)
    
    cols.remove('Id')
    row = {}
    for node in net.nodes:
        row['Id'] = node
        for col in cols:
            e = net.nodes[node][col]
            if isinstance(e, list):
                row[col] = ','.join(e)
            else:
                row[col] = e

        node_pd = node_pd.append(row, ignore_index=True)
    
    
    cols = ['Source', 'Target', 'sublne_c', 'lne_c', 'lne_n', 'lne_num']
    edge_pd = pd.DataFrame(columns = cols)
    
    cols.remove('Source')
    cols.remove('Target')
    row = {}
    for (start, end) in net.edges:
        row['Source'] = start
        row['Target'] = end

        for col in cols:
            e = net.edges[start, end][col]
            if isinstance(e, list):
                row[col] = ','.join(e)
            else:
                row[col] = e
        
        edge_pd = edge_pd.append(row, ignore_index=True)
    return node_pd, edge_pd
# ======================================================================================