# -*- coding: utf-8 -*-
"""
Created on Mon Jun 27 21:08:26 2022

@author: e0751551
"""
#!/usr/bin/env python
import os
import time
import json
import requests
import datetime
import pandas as pd


def print_write(string, path, add_time=True):
    with open(path, 'a') as f:
        if add_time:
            string = '[ {0} ] '.format(datetime.datetime.now()) + string
        print(string, flush=True, file=f)

def pcode_to_data(pcode):
    
    page = 1
    res_all = []
    
    url = 'https://developers.onemap.sg/commonapi/search'
    params = {'searchVal'      : pcode,
              'returnGeom'     : 'Y',
              'getAddrDetails' : 'Y',
              'pageNum'        : page }
    
    while True:
        try:
            time.sleep(0.01)
            response = requests.get(url, params=params)
        except:
            print('Fetching {} failed. Retrying in 5 sec'.format(pcode))
            time.sleep(5)
            continue
        
        content = response.json() 
        res = content['results']
        
        if len(res) == 0:
            break
        
        res_all = res_all + res
        page = page + 1
        
        if page > content['totalNumPages']:
            break
    
    return res_all
# =============================================================================

save_path = 'data/post_code.csv'
logs_path = 'data/logs.csv'

post_code_li = [str(s).zfill(6) for s in range(1000000)]

header = ['SEARCHVAL', 'BLK_NO', 'ROAD_NAME', 'BUILDING', 'ADDRESS', 
          'POSTAL', 'X', 'Y', 'LATITUDE', 'LONGITUDE', 'LONGTITUDE']

# save header
if not os.path.exists(save_path):
    pd.DataFrame(columns=header).to_csv(save_path, header=True, index=False)

for post_code in post_code_li: # 600016
    time.sleep(0.01)
    data = pcode_to_data(post_code)
    
    if len(data) == 0:
        print_write(post_code + ' None', logs_path, add_time=True)
    
    elif len(data) >= 1:
        data_pd = pd.DataFrame(data)[header]
        data_pd.to_csv(save_path, header=False, index=False, mode='a')
        
        print_write(post_code + ' Completed', logs_path, add_time=True)

'''
post_code = '018959'
data = pcode_to_data(post_code)
'''




