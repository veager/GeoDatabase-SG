
import io
import os
import json
import time

import zipfile
import datetime
import requests
import logging

import pandas as pd

# from apscheduler.schedulers.blocking import BlockingScheduler

class ReqDataMallAPI():
    def __init__(self, url, headers, params={}, sleep_sec=0.01):
        '''

        Args:
            url (str): DESCRIPTION.
            headers (dict): DESCRIPTION.
            sleep_sec (float, optional): DESCRIPTION. Defaults to None.

        Returns:
            None.

        '''
        self.url = url
        self.headers = headers
        self.sleep_sec = sleep_sec
        self.params = params
        
        self.max_records_per_page = 500
        
        self.logs_download_pages = []
        return None
    # ----------------------------------------------------------------------
    def _req_download_link(self):
        '''
        obtain the download link from the requested content from "self.url"
        
        '''
        # Request the download link
        req = requests.get(url = self.url, headers = self.headers, 
            params = self.params)
        
        sc, content = req.status_code, req.text
        
        # self.logs_download_link : record the logs of request API
        self.logs_download_link = {'state'              : 'api_request',
                                   'original_url'       : self.url,
                                   'parameters'         : str(self.params),
                                   'request_status_code': sc,
                                   'request_url'        : req.url,
                                   'request_content'    : 'Successful' if sc == 200 else content }
        
        if sc == requests.codes.ok :
            # get the download link
            content = json.loads(content)['value'][0]['Link']  
            
        return content, sc
    # ----------------------------------------------------------------------
    def _req_download_zip(self, link):
        '''
        request the zip file from "link" (url)
        
        Args:
            link (str): the download link of the data

        Returns:
            None.

        '''
        # request the data by download link
        req = requests.get(link)               
        
        # If successfully request, the requested content is the binary format
        sc, content = req.status_code, req.content
        
        # self.logs_download_data : record the logs of request download link
        self.logs_download_zip = {'state'              : 'download_request',
                                   'request_status_code': sc,
                                   'request_url'        : link,
                                   'request_content'    : 'Successful' if sc == 200 else content.decode('ascii') }
        
        # if sc == requests.codes.ok :
        #     # Successful request, the return data is the binary format
        #     content = content
            
        return content, sc
    # ----------------------------------------------------------------------
    def _obtain_data_from_zip(self, content):
        '''
        obtain the data file (.csv) from the zip file ("content")

        Args:
            content (TYPE): DESCRIPTION.

        Returns:
            data_df (pandas.DataFrame) or None: DESCRIPTION.

        '''
        
        with zipfile.ZipFile(io.BytesIO(content), 'r') as zf:
        
            file_name = zf.namelist()[0]
            
            data = zf.read(file_name).decode("utf-8")
        
        data_df = pd.read_csv(io.StringIO(data), header=0, index_col=None)
        
        return data_df, file_name
    # ----------------------------------------------------------------------
    def req_download_zip(self):
        '''

        Returns:
            data_df (pandas.DataFrame) or None: DESCRIPTION.
            file_name (str) or None: DESCRIPTION.

        '''
        time.sleep(self.sleep_sec)
        
        link, link_sc = self._req_download_link()
        
        if link_sc == requests.codes.ok :
            fzip, fzip_sc = self._req_download_zip(link)
            
            if fzip_sc == requests.codes.ok :
                return fzip
        return None
    # ---------------------------------------------------------------------- 
    def req_download_data(self):
        '''

        Returns:
            data_df (pandas.DataFrame) or None: DESCRIPTION.
            file_name (str) or None: DESCRIPTION.

        '''
        link, link_sc = self._req_download_link()
        
        if link_sc == requests.codes.ok :
            fzip, fzip_sc = self._req_download_zip(link)
            
            if fzip_sc == requests.codes.ok :
                data_df, file_name = self._obtain_data_from_zip(fzip)
                return data_df, file_name
        return None, None
    # ----------------------------------------------------------------------
    def request_url_data(self):
        '''
        request the data of url

        Returns:
            data_df (pandas.DataFrame): 
        '''
        req = requests.get(url = self.url, headers = self.headers, params = self.params)
        
        sc, content = req.status_code, req.text
        
        self.request_url_data = {'state'              : 'download_request',
                                 'original_url'       : self.url,
                                 'parameters'         : str(self.params),
                                 'request_status_code': sc,
                                 'request_url'        : req.url,
                                 'request_content'    : 'Successful' if sc == 200 else content.decode('ascii') }
        
        if sc == requests.codes.ok :
            content = json.loads(content)['value']
            data_df = pd.DataFrame(content)
            
        return data_df
    # ----------------------------------------------------------------------
    def _req_one_page(self, no_page=None):
        '''
        Request a designated page 

        Args:
            no_page (int, optional): Defaults to 1.
                the number of page

        Returns:
            content (str): DESCRIPTION.

        '''
        assert (no_page is None) or (no_page >= 0)
        
        params = self.params
        
        if not(no_page is None):
            params.update({'$skip': str(no_page * self.max_records_per_page)})
        
        req = requests.get(url = self.url, headers = self.headers, 
                           params = params)
        
        content, sc = req.text, req.status_code
        
        self.logs_download_pages.append({'no_page'            : no_page,
                                         'original_url'       : self.url,
                                         'parameters'         : str(params),
                                         'request_status_code': sc,
                                         'request_url'        : req.url,
                                         'request_content'    : 'Successful' if sc == 200 else content })
            
        return content, sc
    # ----------------------------------------------------------------------
    def _req_all_page(self, max_page=None):
        '''
        Request all pages and convert them into list type
        
        Args: None
            
        Returns:
            data_all (dict): DESCRIPTION.

        '''
        data_all = []
        
        no_page = 0
        
        while True:
            
            time.sleep(self.sleep_sec)
            
            content, sc = self._req_one_page(no_page=no_page)
            
            # Stop Criterion 1: unsuccessful request
            if sc != 200:
                break
            
            content = json.loads(content)['value']
            # print(len(content))
            data_all.extend(content)
            no_page = no_page + 1
            
            # Stop Criterion 2: no more data
            if len(content) < self.max_records_per_page:
                break
            
            # Stop Criterion 3: reach to the max page
            if (not(max_page is None)) and (no_page > max_page):
                break
        
        return data_all
    # ----------------------------------------------------------------------
    def req_pages_data(self):
        '''

        Returns:
            data_df (pandas.DataFrame) or None: DESCRIPTION.
        '''
        data_all = self._req_all_page()
        
        if len(data_all) == 0:
            return None
        else:
            # dict to pandas.DataFrame
            data_df = pd.DataFrame(data_all)
            return data_df
    # ----------------------------------------------------------------------
    def req_platform_crowd_forecast(self):
        '''

        Returns:
            data_df (pandas.DataFrame): DESCRIPTION.
            date (str):

        '''
        time.sleep(self.sleep_sec)
        
        req = requests.get(url=self.url, headers=self.headers, params=self.params)
        
        content = req.text 
        data = json.loads(content)['value'][0]
        dt = data['Date']
        
        data_df = pd.DataFrame(columns=['Station', 'Start', 'CrowdLevel'])
        
        for sta in data['Stations']:
            data_sta = pd.DataFrame(sta['Interval'])
            data_sta['Station'] = sta['Station']
            
            data_df = data_df.append(data_sta, ignore_index=True)
        return data_df, dt
    # ----------------------------------------------------------------------
    def req_platform_crowd_realtime(self):
        '''

        Returns:
            data_df (pandas.DataFrame): DESCRIPTION.
            date (str):

        '''
        time.sleep(self.sleep_sec)
        
        req = requests.get(url=self.url, headers=self.headers, params=self.params)
        
        content = req.text 
        
        data = json.loads(content)['value']
        
        data_df = pd.DataFrame(data)
        
        return data_df
    # ----------------------------------------------------------------------
    def _req_geospatial_params(self):
        '''

        Returns:
            params_df (pandas.DataFrame): DESCRIPTION.

        '''
        url = 'https://raw.githubusercontent.com/veager/GeoDatabase-Singapore/main/DataMall/Parameters/V5.4_ANNEX_E.txt'
        params_df = pd.read_csv(url, header=0, index_col=None, comment='#')
        
        return params_df
    # ----------------------------------------------------------------------
    def _req_train_line_params(self):
        '''

        Returns:
            params_df (pandas.DataFrame): DESCRIPTION.

        '''
        url = 'https://raw.githubusercontent.com/veager/GeoDatabase-Singapore/main/DataMall/Parameters/V5.4_Train_Line.txt'
        
        params_df = pd.read_csv(url, header=0, index_col=None, sep=',')
        
        return params_df
# ==========================================================================




def save_data_df(path, data):
    
    folder_path = os.path.dirname(path)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    data.to_csv(path, index=False)
    return None
# ============================================================================




HEADERS = {'AccountKey': '+mx8ADrbR8WWO4x6/kewfw==',
           'accept':     'application/json'}

ROOT_PATH = os.path.join(os.getcwd(), 'data')



# + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + 
#   
#   
#   2.2  BUS SERVICES                     Update Freq : Ad hoc, as monthly
#   2.3  BUS ROUTES                       Update Freq : Ad hoc, as monthly
#   2.4  BUS STOPS                        Update Freq : Ad hoc, as monthly
#   2.13 ERP RATES                        Update Freq : Ad hoc, as monthly
#
#   2.10 TAXI STANDS                      Update Freq : monthly
#
#   2.23 GEOSPATIAL WHOLE ISLAND          Update Freq : Ad-hoc, as monthly
#           Parameters: ID = ArrowMarking
#                       https://raw.githubusercontent.com/veager/GeoDatabase-Singapore/main/DataMall/Parameters/V5.4_ANNEX_E.txt
#    
# + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + +


def save_pages_data():
    
    req_urls = [{'url'         : 'http://datamall2.mytransport.sg/ltaodataservice/BusServices',
                 'folder_name' : '2_2_BUS_SERVICES',
                 'file_name'   : ''
                 },
                {'url'         : 'http://datamall2.mytransport.sg/ltaodataservice/BusRoutes',
                 'folder_name' : '2_3_BUS_ROUTES',
                 'file_name'   : ''
                 },
                {'url'         : 'http://datamall2.mytransport.sg/ltaodataservice/BusStops',
                 'folder_name' : '2_4_BUS_STOPS',
                 'file_name'   : ''
                 },
                {'url'         : 'http://datamall2.mytransport.sg/ltaodataservice/ERPRates',
                 'folder_name' : '2_13_ERP_RATES',
                 'file_name'   : ''},
                ]
              
    for info in req_urls:
        rdm = ReqDataMallAPI(url = info['url'], headers = HEADERS)
        data_df = rdm.req_pages_data()
        
        dt = datetime.datetime.now()
        
        save_path = os.path.join(ROOT_PATH, 
                                 info['folder_name'], 
                                '{0}.csv'.format(dt.strftime("%Y-%m-%d"))
                                )
        save_data_df(path = save_path, data = data_df)
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - -
    
    url = 'http://datamall2.mytransport.sg/ltaodataservice/TaxiStands'
    folder_name = '2_10_TAXI_STANDS'
    
    dt = datetime.datetime.now()
    
    file_name = '{0}_{1}.csv'.format('taxi_stands', dt.strftime("%Y-%m-%d"))
    save_path = os.path.join(ROOT_PATH, folder_name, file_name)
    
    req = ReqDataMallAPI(url, headers=HEADERS)
    data_df = req.request_url_data()
    
    save_data_df(save_path, data_df)
    return None
# =============================================================================


def save_geospatial():
    
    url = 'http://datamall2.mytransport.sg/ltaodataservice/GeospatialWholeIsland'
    folder_name = '2_23_GEOSPATIAL'
    
    dt_str = datetime.datetime.now().strftime('%Y-%m')
    
    save_folder = os.path.join(ROOT_PATH, folder_name, dt_str)
    
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)
    
    # Request parameters
    params_df = ReqDataMallAPI(url=None , headers=None)._req_geospatial_params()
    
    for ix, row in params_df.iterrows():
        
        # if row['ID'] != 'PedestrainOverheadbridge_Underpass':
        #     continue
        
        params = {'ID': row['ID']}
        rdm = ReqDataMallAPI(url=url, headers=HEADERS, params=params, sleep_sec=1.)
        content = rdm.req_download_zip()
        
        if content is None:
            print(rdm.logs_download_link['parameters'])
            print(rdm.logs_download_link['request_content'])
            print(rdm.logs_download_link['request_status_code'])
            continue
        
        file_path = os.path.join(save_folder, '{0}.zip'.format(row['ID']))
        with open(file_path, 'wb') as f:
            f.write(content)
    
    return None
# =============================================================================



# + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + 
#   
#   
#   2.5 PASSENGER VOLUME BY BUS STOPS                              Update Freq : monthly
#   2.6 PASSENGER VOLUME BY ORIGIN DESTINATION BUS STOPS           Update Freq : monthly
#   2.7 PASSENGER VOLUME BY ORIGIN DESTINATION TRAIN STATIONS      Update Freq : monthly
#   2.8 PASSENGER VOLUME BY TRAIN STATIONS                         Update Freq : monthly
#       parameters:   Date: 202204
#
# + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + 


def save_passenger_data():

    dt = datetime.datetime.now() - datetime.timedelta(days=30)
    params = {'Date': dt.strftime("%Y%m")}
    
    req_urls = [{'url'         : 'http://datamall2.mytransport.sg/ltaodataservice/PV/Bus',
                 'folder_name' : '2_5_PVOL_BUS_STOPS'
                 },
                {'url'         : 'http://datamall2.mytransport.sg/ltaodataservice/PV/ODBus',
                 'folder_name' : '2_6_PVOL_BUS_OD'
                 },
                {'url'         : 'http://datamall2.mytransport.sg/ltaodataservice/PV/ODTrain',
                 'folder_name' : '2_7_PVOL_TRAIN_OD'  
                 },
                {'url'         : 'http://datamall2.mytransport.sg/ltaodataservice/PV/Train',
                 'folder_name' : '2_8_PVOL_TRAIN_STATIONS'}]
    
    for info in req_urls:
        try:
            rdm = ReqDataMallAPI(url = info['url'], headers = HEADERS, params = params)
            data_df, file_name = rdm.req_download_data()
            
            save_path = os.path.join(ROOT_PATH, 
                                     info['folder_name'],
                                     params['Date'][:4],   # Year
                                     file_name)
            
            save_data_df(path = save_path, data = data_df)
        except:
            print('Error:', info['folder_name'])

    return None
# =============================================================================



# + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + 
#   
#   
#   2.9  TAXI AVAILABILITYS                  Update Freq : 1 minute
#   2.12 CARPARK AVAILABILITY                Update Freq : 1 minute
# 
#   2.14 ESTIMATED TRAVEL TIMES              Update Freq : 5 minutes   
#   2.20 TRAFFIC SPEED BANDS                 Update Freq : 5 minutes  
# 
#   2.16 ROAD OPENINGS                       Update Freq : 24 hours
#   2.17 ROAD WORKS                          Update Freq : 24 hours
#
#   2.19 TRAFFIC INCIDENTS                   Update Freq : 2 minutes
#   2.21 VMS / EMAS                          Update Freq : 2 minutes
#   
#   2.25 PLATFORM CROWD DENSITY REAL TIME    Update Freq : 10 minutes
#   2.26 PLATFORM CROWD DENSITY FORECAST     Update Freq : 24 hours
#           Parameters: TrainLine = NSL
#                       https://raw.githubusercontent.com/veager/GeoDatabase-Singapore/main/DataMall/Parameters/V5.4_Train_Line.txt
# 
#   2.11 TRAIN SERVICE ALERTS                Update Freq : Ad hoc, daily
#   2.24 FACILITIES MAINTENANCE              Update Freq : Ad hoc, daily
# 
# + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + 


def save_taxi_avail():
    try:
        url = 'http://datamall2.mytransport.sg/ltaodataservice/Taxi-Availability'
        folder_name = '2_9_TAXI_AVAILABILITYS'
        
        dt = datetime.datetime.now()
        file_name = '{0}.csv'.format(dt.strftime("%Y-%m-%d-%H-%M-%S"))
        save_path = os.path.join(ROOT_PATH, folder_name, dt.strftime("%Y-%m-%d"), file_name)
        
        req = ReqDataMallAPI(url, headers=HEADERS)
        data_df = req.req_pages_data()
        
        save_data_df(save_path, data_df)
    except:
        print('2_9_TAXI_AVAILABILITYS')

    return None
    


def save_carpark_avail():
    try:
        url = 'http://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2'
        folder_name = '2_12_CARPARK_AVAILABILITY'
        
        dt = datetime.datetime.now()
        file_name = '{0}.csv'.format(dt.strftime("%Y-%m-%d-%H-%M-%S"))
        save_path = os.path.join(ROOT_PATH, folder_name, dt.strftime("%Y-%m-%d"), file_name)
        
        req = ReqDataMallAPI(url, headers=HEADERS)
        data_df = req.req_pages_data()
        
        save_data_df(save_path, data_df)
    except:
        print('Error: 2_12_CARPARK_AVAILABILITY')

    return None



def save_travel_time():
    try:
        # = = = travel time
        url = 'http://datamall2.mytransport.sg/ltaodataservice/EstTravelTimes'
        folder_name = '2_14_TRAVEL_TIMES'
        
        dt = datetime.datetime.now()
        file_name = '{0}.csv'.format(dt.strftime("%Y-%m-%d-%H-%M-%S"))
        save_path = os.path.join(ROOT_PATH, folder_name, dt.strftime("%Y-%m-%d"), file_name)
        
        req = ReqDataMallAPI(url, headers=HEADERS)
        data_df = req.req_pages_data()
        
        save_data_df(save_path, data_df)
    except:
        print('Error: 2_14_TRAVEL_TIMES')

  
# = = = traffic speed
def save_traffic_speed():
    try:
        url = 'http://datamall2.mytransport.sg/ltaodataservice/TrafficSpeedBandsv2'
        folder_name = '2_20_TRAFFIC_SPEED'
        
        dt = datetime.datetime.now()
        file_name = '{0}.csv'.format(dt.strftime("%Y-%m-%d-%H-%M-%S"))
        save_path = os.path.join(ROOT_PATH, folder_name, dt.strftime("%Y-%m-%d"), file_name)
        
        req = ReqDataMallAPI(url, headers=HEADERS)
        data_df = req.req_pages_data()
        
        save_data_df(save_path, data_df)
    except:
        print('Error: 2_20_TRAFFIC_SPEED')
    return None



def save_road_openning_work():  # Update Freq : 24 hours
    # = = = road opennings
    try: 
        url = 'http://datamall2.mytransport.sg/ltaodataservice/RoadOpenings'
        folder_name = '2_16_ROAD_OPENINGS'
        
        dt = datetime.datetime.now()
        file_name = '{0}.csv'.format(dt.strftime("%Y-%m-%d-%H-%M-%S"))
        save_path = os.path.join(ROOT_PATH, folder_name, dt.strftime("%Y-%m"), file_name)
        
        req = ReqDataMallAPI(url, headers=HEADERS)
        data_df = req.req_pages_data()
        
        save_data_df(save_path, data_df)
    except:
        print('Error: 2_16_ROAD_OPENINGS')
    
    # = = = road works
    try:
        url = 'http://datamall2.mytransport.sg/ltaodataservice/RoadWorks'
        folder_name = '2_17_ROAD_WORKS'
        
        dt = datetime.datetime.now()
        file_name = '{0}.csv'.format(dt.strftime("%Y-%m-%d-%H-%M-%S"))
        save_path = os.path.join(ROOT_PATH, folder_name, dt.strftime("%Y-%m"), file_name)
        
        req = ReqDataMallAPI(url, headers=HEADERS)
        data_df = req.req_pages_data()
        
        save_data_df(save_path, data_df)
    except:
        print('Error: 2_17_ROAD_WORKS')
        
    return None



def save_traffic_incident_vms():

    # = = = traffic incident
    try:
        url = 'http://datamall2.mytransport.sg/ltaodataservice/TrafficIncidents'
        folder_name = '2_19_TRAFFIC_INCIDENTS'
        
        dt = datetime.datetime.now()
        file_name = '{0}.csv'.format(dt.strftime("%Y-%m-%d-%H-%M-%S"))
        save_path = os.path.join(ROOT_PATH, folder_name, dt.strftime("%Y-%m-%d"), file_name)
        
        req = ReqDataMallAPI(url, headers=HEADERS)
        data_df = req.req_pages_data()
        
        save_data_df(save_path, data_df)
    except:
        print('Error: 2_19_TRAFFIC_INCIDENTS')

    
    try:
        # = = = VMS / EMAS
        url = 'http://datamall2.mytransport.sg/ltaodataservice/VMS'
        folder_name = '2_21_VMS_EMAS'
        
        dt = datetime.datetime.now()
        file_name = '{0}.csv'.format(dt.strftime("%Y-%m-%d-%H-%M-%S"))
        save_path = os.path.join(ROOT_PATH, folder_name, dt.strftime("%Y-%m-%d"), file_name)
        
        req = ReqDataMallAPI(url, headers=HEADERS)
        data_df = req.req_pages_data()
        
        save_data_df(save_path, data_df)
    except:
        print('Error: 2_21_VMS_EMAS')

    return None



def save_platform_crowd_realtime():     # Update Freq : 10 minutes
    '''
    request the real-time platform crowd of all stations
    '''
    try:
        url = 'http://datamall2.mytransport.sg/ltaodataservice/PCDRealTime'
        folder_name = '2_25_PLATFORM_CROWD_REAL_TIME'
        
        dt = datetime.datetime.now()
        file_name = '{0}.csv'.format(dt.strftime("%Y-%m-%d-%H-%M-%S"))
        save_path = os.path.join(ROOT_PATH, folder_name, dt.strftime("%Y-%m-%d"), file_name)
        
        params_df = ReqDataMallAPI(url=None, headers=None)._req_train_line_params()
        
        data_all_df = []
        for ix, row in params_df.iterrows():
            params = {'TrainLine': row['Train Line Code']}
            
            rdm = ReqDataMallAPI(url = url, headers = HEADERS, params = params)
            data_df = rdm.req_platform_crowd_realtime()

            data_all_df.append(data_df)
        data_all_df = pd.concat(data_all_df, ignore_index=True, axis=0)

        save_data_df(save_path, data_all_df)
    except:
        print('Error: 2_25_PLATFORM_CROWD_REAL_TIME')

    return None


def save_platform_crowd_forecast():  # Update Freq : 24 hours
    '''
    request the forecasted platform crowd of all stations
    '''
    try:
        url = 'http://datamall2.mytransport.sg/ltaodataservice/PCDForecast'
        folder_name = '2_26_PLATFORM_CROWD_FORECAST'
        
        dt = datetime.datetime.now()
        file_name = '{0}.csv'.format(dt.strftime("%Y-%m-%d-%H-%M-%S"))
        save_path = os.path.join(ROOT_PATH, folder_name, dt.strftime("%Y-%m"), file_name)
        
        params_df = ReqDataMallAPI(url=None, headers=None)._req_train_line_params()
        
        data_all_df = []
        for ix, row in params_df.iterrows():
            params = {'TrainLine': row['Train Line Code']}
            
            rdm = ReqDataMallAPI(url = url, headers = HEADERS, params = params)
            data_df = rdm.req_platform_crowd_forecast()

            data_all_df.append(data_df)
        data_all_df = pd.concat(data_all_df, ignore_index=True, axis=0)

        save_data_df(save_path, data_all_df)
    except:
        print('Error: 2_26_PLATFORM_CROWD_FORECAST')
        
    return None
# ==========================================================================