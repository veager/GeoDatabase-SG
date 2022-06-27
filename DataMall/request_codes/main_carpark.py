from RequestDataMallAPI import *



'''
# Update Freq : monthly
save_pages_data()
save_geospatial()
save_passenger_data()
'''

from apscheduler.schedulers.blocking import BlockingScheduler

def initial_job():

    save_taxi_avail()      # Update Freq : 1 minute
    save_carpark_avail()   # Update Freq : 1 minute
    
    save_travel_time()     # Update Freq : 5 minutes 
    save_traffic_speed()   # Update Freq : 5 minutes
    
    # save_traffic_incident_vms()         # Update Freq : 2 minutes
    
    # save_road_openning_work()           # Update Freq : 24 hours
    
    # save_platform_crowd_realtime()      # Update Freq : 10 minutes
    
    # save_platform_crowd_forecast()      # Update Freq : 24 hours


def scheduler_job():
    # BlockingScheduler
    scheduler = BlockingScheduler()

    # car park, 
    # 1 minute
    scheduler.add_job(save_taxi_avail,    'interval', minutes=1, id='save_taxi_avail')
    scheduler.add_job(save_carpark_avail, 'interval', minutes=1, id='save_carpark_avail')
    
    # traffic incident, VMS
    # 2 minute
    # scheduler.add_job(save_traffic_incident_vms, 'interval', minutes=2, id='save_traffic_incident_vms')
    
    # traffic speed, 5 minute
    scheduler.add_job(save_travel_time,   'interval', minutes=5, id='save_travel_time')
    scheduler.add_job(save_traffic_speed, 'interval', minutes=5, id='save_traffic_speed')
    
    # 10 minutes
    # scheduler.add_job(save_platform_crowd_realtime, 'interval', minutes=10, id='save_platform_crowd_realtime')

    # 24 hours
    # scheduler.add_job(save_road_openning_work, 'interval', hours=24, id='save_road_openning_work')
    # scheduler.add_job(save_platform_crowd_forecast, 'interval', hours=24, id='save_platform_crowd_realtime')
    
    scheduler.start()



print('Start...')

initial_job()

scheduler_job()
