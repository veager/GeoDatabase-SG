import pandas as pd
import geopandas as gpd
import networkx as nx


def ReadBusRoute(path):
    data = pd.read_csv(
        path, index_col=0, dtype={'BusStopCode':str}
    )
    data = data.iloc[:, :6]
    # route_no = route_data['ServiceNo'].value_counts()
    # stop_no = route_data['BusStopCode'].value_counts()
    return data
# --------------------------------------------------------
def BusRoute2Net(route_data):
    '''
    According to bus route table, construct the bus networks

    Parameters
    ----------
    route_data : pandas.DataFrame
        DESCRIPTION.

    Returns
    -------
    net : networkx.DiGraph
        Directed bus networks

    '''
    net = nx.DiGraph()
    
    # node list
    node_li = route_data['BusStopCode'].unique().tolist()
    node_li.sort()
    # print(len(stop_no_li))
    # stop bus operator information
    node_operator = route_data[['ServiceNo', 'BusStopCode']].drop_duplicates()
    
    # add bus stop
    for node in node_li:
        
        net.add_node(node)
        # add node accociated bus line information
        serv_li = node_operator[node_operator['BusStopCode'] == node]['ServiceNo']
        serv_li = serv_li.drop_duplicates().to_list()
        net.nodes[node]['serv_li'] = ','.join(serv_li)
        net.nodes[node]['serv_no'] = len(serv_li)
        
    # add bus route
    # bus route list
    bus_line = route_data['ServiceNo'].unique().tolist()
    
    for serv_ix in bus_line:
        route = route_data[route_data['ServiceNo'] == serv_ix]
        route = route.sort_values(by=['Direction', 'StopSequence'])
        # directions list [1,2] or [1]
        directions = route['Direction'].unique().tolist()
        
        for d_ix in directions:
            # select one-direction bus line
            route_dir = route[route['Direction'] == d_ix]
            route_dir = route_dir.sort_values(by='StopSequence')
            route_dir = route_dir.reset_index(drop=True)
            
            for i in range(route_dir.shape[0]-1):
                start_node = route_dir.loc[i, 'BusStopCode']
                end_node = route_dir.loc[i+1, 'BusStopCode']
                dist = route_dir.loc[i+1, 'Distance'] - route_dir.loc[i, 'Distance']
                # a new route between two stop node
                if not net.has_edge(start_node, end_node):
                    net.add_edge(start_node, end_node)
                    net.edges[start_node, end_node]['serv_li'] = [serv_ix]
                    net.edges[start_node, end_node]['distance'] = dist
                # already existing route
                else:
                    serv_li = net.edges[start_node, end_node]['serv_li']
                    if not (serv_ix in serv_li):
                        serv_li.append(serv_ix)
                        net.edges[start_node, end_node]['serv_li'] = serv_li
     
    # print(len(net.nodes), len(net.edges))
    return net
# --------------------------------------------------------
def BusNetAddLoc(net, node_loc):
    '''
    add node location information for bus networks

    Parameters
    ----------
    net : networkx.DiGraph()
        DESCRIPTION.
    node_loc : pandas.DataFrame
        DESCRIPTION.

    Returns
    -------
    net : TYPE
        DESCRIPTION.

    '''
    node_li_loc = set(node_loc['BusStopCode'].unique().tolist())
    node_li_route = set(list(net.nodes))
    
    node_li_onlyin_loc = list(node_li_loc.difference(node_li_route))
    node_li_onlyin_route = list(node_li_route.difference(node_li_loc))
    # all node list                        
    all_node_li = list(node_li_loc.union(node_li_route))
    
    node_li_onlyin_loc.sort()
    node_li_onlyin_route.sort()
    all_node_li.sort()
    
    # add bus stop
    for node in all_node_li:
        # nodes which only in location
        if node in node_li_onlyin_loc:
            net.add_node(node)
            net.nodes[node]['serv_li'] = ''
            net.nodes[node]['serv_no'] = ''
            # add stop location infomation
            node_info = node_loc[node_loc['BusStopCode'] == node]
            net.nodes[node]['lng'] = node_info['geometry'].x.values[0]
            net.nodes[node]['lat'] = node_info['geometry'].y.values[0]
        # nodes which only in route
        elif node in node_li_onlyin_route:
            net.nodes[node]['lng'] = ''
            net.nodes[node]['lat'] = ''
        # nodes which both in location and rounte
        else: 
            # add stop location infomation
            node_info = node_loc[node_loc['BusStopCode'] == node]
            net.nodes[node]['lng'] = node_info['geometry'].x.values[0]
            net.nodes[node]['lat'] = node_info['geometry'].y.values[0]
    return net
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
        columns = ['Id', 'lat', 'lng', 'serv_no', 'serv_li']
    )
    row = {}
    for node in net.nodes:
        row['Id'] = node
        row['lat'] = net.nodes[node]['lat']
        row['lng'] = net.nodes[node]['lng']
        row['serv_no'] = net.nodes[node]['serv_no']
        row['serv_li'] = net.nodes[node]['serv_li']
        node_pd = node_pd.append(row, ignore_index=True)
    
    edge_pd = pd.DataFrame(
        columns = ['Source', 'Target', 'Distance', 'serv_no', 'serv_li']
    )
    row = {}
    for (start, end) in net.edges:
        row['Source'] = start
        row['Target'] = end
        row['Distance'] = net.edges[start, end]['distance']
        row['serv_li'] = ','.join(net.edges[start, end]['serv_li'])
        row['serv_no'] = len(net.edges[start, end]['serv_li'])
        edge_pd = edge_pd.append(row, ignore_index=True)
    return node_pd, edge_pd
# --------------------------------------------------------
def NetProcessing(net):
    # remove self loops
    net.remove_edges_from(nx.selfloop_edges(net))
    # remove islotate nodes
    net.remove_nodes_from(list(nx.isolates(net)))
    
    # no_loc = 0
    # delete the stop without location info
    # and construct the edge across that stop
    delete_node_li = []
    for node in net.nodes:
        if net.nodes[node]['lat'] == '':
            # no_loc = no_loc + 1
            node_prede = list(net.predecessors(node))
            # successive nodes
            node_succe = list(net.successors(node))
            if len(node_succe) == 0:
                delete_node_li.append(node)
            elif len(node_prede) == 0:
                delete_node_li.append(node)
            else:
                delete_node_li.append(node)
                # add edge
                for _np in node_prede:
                    for _ns in node_succe:
                        if _np == _ns:
                            continue
                        # print(_np, _ns)
                        net.add_edge(_np, _ns)
                        net.edges[_np, _ns]['distance'] = net.edges[_np, node]['distance'] + net.edges[node, _ns]['distance']
                        serv_li_p = net.edges[_np, node]['serv_li']
                        serv_li_s = net.edges[node, _ns]['serv_li']
                        serv_li = list(set(serv_li_p).intersection(serv_li_s))
                        net.edges[_np, _ns]['serv_li'] = ','.join(serv_li)
                        net.edges[_np, _ns]['serv_no'] = len(serv_li)
                        if len(serv_li) == 0:
                            net.remove_edge(_np, _ns)
    # delete the node
    net.remove_nodes_from(delete_node_li)
    return net
# --------------------------------------------------------


def Route2Shp(route_data, bus_stop_loc):
    '''
    transform the bus route to shapefile (Lines layer)

    Parameters
    ----------
    route_data: pandas.DataFrame
        DESCRIPTION.
        
    bus_stop_loc: geopandas.GeoDataFrame
        DESCRIPTION.
        
    Returns
    -------
    route_shp : geopandas.GeoDataFrame
        DESCRIPTION.

    '''
    from shapely.geometry import LineString
    
    # merge 
    route_data = route_data.merge(right=bus_stop_loc, 
        on='BusStopCode', how = 'left'
    )
    #
    route_data.sort_values(
        by = ['Operator', 'ServiceNo', 'Direction', 'StopSequence'],
        inplace = True
    )
    # route_data[route_data['Description'].isna()]
    
    # drop stop without location info
    route_data.dropna(subset=['geometry'], axis=0, inplace=True)

    # route shapfile, line layer
    route_shp = gpd.GeoDataFrame(
        columns = ['Operator', 'ServiceNo', 'Direction', 'StopSequence', 'Distance', 'geometry'],
        crs = bus_stop_loc.crs
    )
    
    row = {}
    # one bus line group
    for group in route_data.groupby(by=['ServiceNo', 'Direction']):
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
# =============================================================================
