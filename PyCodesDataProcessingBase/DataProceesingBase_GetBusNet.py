import numpy as np
import pandas as pd
import networkx as nx
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
    # 
    data2 = data2[data1.columns]
    # concat two data
    data = pd.concat([data1, data2], axis=0, join='outer', ignore_index=True )
    
    # drop duplicate
    data.drop_duplicates(subset='BusStopCode', inplace=True, ignore_index=True)
    
    data.sort_values(by='BusStopCode', ignore_index=True, inplace=True)
    
    return data
# =============================================================================

def ReadBusRouteCsv(path):
    '''

    Parameters
    ----------
    path : str
        DESCRIPTION.

    Returns
    -------
    data : pandas.DataFrame
        DESCRIPTION.

    '''
    data = pd.read_csv(path, index_col=0, 
        dtype={'ServiceNo'    : str, 
               'Operator'     : str,
               'Direction'    : int,
               'BusStopCode'  : str,
               'StopSequence' : int,
               'BusStopCode'  : str,
               'Distance'     : float} 
    )
    
    data = data.iloc[:, :6]
    # route_no = route_data['ServiceNo'].value_counts()
    # stop_no = route_data['BusStopCode'].value_counts()
    return data
# --------------------------------------------------------
def BusStopRoute2Net(stop, route, multiedge=False):
    '''
    According to bus route table, construct the bus networks

    Parameters
    ----------
    stop : geopandas.GeoDataFrame
        DESCRIPTION.
    
    route : pandas.DataFrame
        DESCRIPTION.
    
    Returns
    -------
    net : networkx.DiGraph
        Directed bus networks

    '''
    # drop the stop not in route or in bus_stop
    # because there lack the info 
    route = route.merge(right=stop[['BusStopCode', 'geometry']], 
        on='BusStopCode', how='inner')
    route.sort_values(by=['ServiceNo', 'Direction', 'StopSequence'])
    
    # bus line list
    bus_line_li = route['ServiceNo'].unique().tolist()
    
    if multiedge:
        # directed multiedge graph
        net = nx.MultiDiGraph()
        
        # add bus route
        for serv_ix in bus_line_li:
            one_route = route[route['ServiceNo'] == serv_ix]
            # directions list [1,2] or [1]
            directions = route['Direction'].unique().tolist()

            for d_ix in directions:
                # select one-direction bus line
                route_dir = one_route[one_route['Direction'] == d_ix]
                route_dir = route_dir.sort_values(by='StopSequence')
                route_dir.reset_index(drop=True, inplace=True)
                
                for i in range(route_dir.shape[0]-1):
                    start_node = route_dir.loc[i, 'BusStopCode']
                    end_node = route_dir.loc[i+1, 'BusStopCode']
                    dist = route_dir.loc[i+1, 'Distance'] - route_dir.loc[i, 'Distance']
                    
                    net.add_edge(start_node, end_node, serv_ix)
                    net.edges[start_node, end_node, serv_ix]['serv'] = serv_ix
                    net.edges[start_node, end_node, serv_ix]['oprt'] = route_dir.loc[i, 'Operator']
                    net.edges[start_node, end_node, serv_ix]['dist'] = dist
    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  - 
    else:
        # directed graph
        net = nx.DiGraph()
        
        # add bus route
        for serv_ix in bus_line_li:
            one_route = route[route['ServiceNo'] == serv_ix]
            # directions list [1,2] or [1]
            directions = route['Direction'].unique().tolist()

            for d_ix in directions:
                # select one-direction bus line
                route_dir = one_route[one_route['Direction'] == d_ix]
                route_dir = route_dir.sort_values(by='StopSequence')
                route_dir.reset_index(drop=True, inplace=True)

                for i in range(route_dir.shape[0]-1):
                    start_node = route_dir.loc[i, 'BusStopCode']
                    end_node = route_dir.loc[i+1, 'BusStopCode']
                    dist = route_dir.loc[i+1, 'Distance'] - route_dir.loc[i, 'Distance']
                    
                    # a new route between two stop node
                    if not net.has_edge(start_node, end_node):
                        net.add_edge(start_node, end_node)
                        net.edges[start_node, end_node]['dist'] = dist
                        net.edges[start_node, end_node]['serv_li'] = [serv_ix]
                    # already existing route
                    else:
                        serv_li = net.edges[start_node, end_node]['serv_li'].copy()
                        serv_li.append(serv_ix)
                        serv_li = list(set(serv_li))
                        net.edges[start_node, end_node]['serv_li'] = serv_li.copy()
                        net.edges[start_node, end_node]['serv_num'] = len(serv_li)
    # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  - 
    
    # add bus stop information
    route = route[['BusStopCode', 'ServiceNo', 'geometry']]
    # drop duplicate
    route.drop_duplicates(subset=['BusStopCode', 'ServiceNo'], inplace=True, ignore_index=True)
    route.sort_values(by='BusStopCode', ignore_index=True, inplace=True)
    route = gpd.GeoDataFrame(route, crs=stop.crs)
    
    for group in route.groupby(by='BusStopCode'):
        (node, group) = group
        serv_li = group['ServiceNo'].unique().tolist()
        net.nodes[node]['serv_li'] = serv_li
        net.nodes[node]['serv_num'] = len(serv_li)
        
        net.nodes[node]['lng'] = group['geometry'].x.values[0]
        net.nodes[node]['lat'] = group['geometry'].y.values[0]
    return net
# --------------------------------------------------------
def NetProcessing(net):
    # remove self loops
    net.remove_edges_from(nx.selfloop_edges(net))
    # remove islotate nodes
    net.remove_nodes_from(list(nx.isolates(net)))
    
    # no_loc = 0
    # delete the stop without location info
    # and construct the edge across that stop
    # delete_node_li = []
    # for node in net.nodes:
    #     if net.nodes[node]['lat'] == '':
    #         # no_loc = no_loc + 1
    #         node_prede = list(net.predecessors(node))
    #         # successive nodes
    #         node_succe = list(net.successors(node))
    #         if len(node_succe) == 0:
    #             delete_node_li.append(node)
    #         elif len(node_prede) == 0:
    #             delete_node_li.append(node)
    #         else:
    #             delete_node_li.append(node)
    #             # add edge
    #             for _np in node_prede:
    #                 for _ns in node_succe:
    #                     if _np == _ns:
    #                         continue
    #                     # print(_np, _ns)
    #                     net.add_edge(_np, _ns)
    #                     net.edges[_np, _ns]['distance'] = net.edges[_np, node]['distance'] + net.edges[node, _ns]['distance']
    #                     serv_li_p = net.edges[_np, node]['serv_li']
    #                     serv_li_s = net.edges[node, _ns]['serv_li']
    #                     serv_li = list(set(serv_li_p).intersection(serv_li_s))
    #                     net.edges[_np, _ns]['serv_li'] = ','.join(serv_li)
    #                     net.edges[_np, _ns]['serv_no'] = len(serv_li)
    #                     if len(serv_li) == 0:
    #                         net.remove_edge(_np, _ns)
    # # delete the node
    # net.remove_nodes_from(delete_node_li)
    return net
# --------------------------------------------------------
def Route2LineShp(stop, route):
    '''
    transform the bus route to bus line shapefile (Lines layer)
        each bus line (including many bus routes) is a line object in shapefile

    Parameters
    ---------- 
    stop: geopandas.GeoDataFrame
        DESCRIPTION.
    
    route: pandas.DataFrame
        DESCRIPTION.

        
    Returns
    -------
    route_shp : geopandas.GeoDataFrame
        DESCRIPTION.

    '''
    from shapely.geometry import LineString
    
    # drop the stop not in route or in bus_stop
    # because there lack the info 
    route = route.merge(right=stop[['BusStopCode', 'geometry']], 
        on='BusStopCode', how='inner')
    route.sort_values(by=['ServiceNo', 'Direction', 'StopSequence'])
    #
    route.sort_values(by = ['Operator', 'ServiceNo', 'Direction', 'StopSequence'],
        inplace = True
    )
    
    # route shapfile, line layer
    route_shp = gpd.GeoDataFrame(
        columns = ['Operator', 'ServiceNo', 'Direction', 'StopSequence', 'Distance', 'geometry'],
        crs = stop.crs
    )
    
    row = {}
    # one bus line group
    for group in route.groupby(by=['ServiceNo', 'Direction']):
        (_, group) = group
        # the number of stops in one bus line group
        n = group.shape[0]
        group.reset_index(drop=True, inplace=True)
        group.sort_values(by='StopSequence', inplace=True)

        row['Operator'] = group['Operator'][0]
        row['ServiceNo'] = group['ServiceNo'][0]
        row['Direction'] = group['Direction'][0]
        row['StopSequence'] = ','.join(group['BusStopCode'].to_list())
        row['Distance'] = group['Distance'][n-1] - group['Distance'][0]
        row['geometry'] = LineString(group['geometry'].to_list())

        route_shp = route_shp.append(row, ignore_index=True)
    
    return route_shp
# --------------------------------------------------------
def BusNet2NodeEgesPd(net):
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
    
    node_pd = pd.DataFrame(
        columns = ['Id', 'lat', 'lng', 'serv_num', 'serv_li']
    )
    row = {}
    for node in net.nodes:
        row['Id'] = node
        row['lat'] = net.nodes[node]['lat']
        row['lng'] = net.nodes[node]['lng']
        row['serv_num'] = net.nodes[node]['serv_num']
        row['serv_li'] = ','.join(net.nodes[node]['serv_li'])
        node_pd = node_pd.append(row, ignore_index=True)
    
    edge_pd = pd.DataFrame(
        columns = ['Source', 'Target', 'dist', 'serv_num', 'serv_li']
    )
    row = {}
    for (start, end) in net.edges:
        row['Source'] = start
        row['Target'] = end
        row['dist'] = net.edges[start, end]['dist']
        row['serv_num'] = len(net.edges[start, end]['serv_li'])
        row['serv_li'] = ','.join(net.edges[start, end]['serv_li'])
        edge_pd = edge_pd.append(row, ignore_index=True)
    return node_pd, edge_pd
# --------------------------------------------------------
# =============================================================================