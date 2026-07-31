 # timedelta_directioncount_new.py
 # This script consumes messages from a Kafka topic named 'direction', 
 # processes the data to count vehicle and pedestrian movements in various directions, 
 # and produces aggregated results back to a Kafka topic named 'directioncount'.


from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import time
import datetime
import pytz
from datetime import datetime, timedelta
import time

global final_op
final_op =[]
        
# Set up Kafka consumer
# Kafka broker address
bootstrap_servers = 'localhost:9092'
# Kafka topic to which you want to send the data
topic = 'directioncount'
# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

#consumer2= KafkaConsumer('directioncount', bootstrap_servers='localhost:9092')
consumer = KafkaConsumer('direction', bootstrap_servers='localhost:9092')

### time zone
est = pytz.timezone('America/New_York')

####camera stuff
cameras = set()
recent_crossings = {}
crossing_cooldown_seconds = 3
#polygon_dictionary_ped = set()
directions = ["nn", "ns", "ne", "nw", "ss", "sn", "se", "sw", "ee", "en", "es", "ew", "ww", "wn", "ws", "we"]
################ change this to add more bike/ped only cameras ################
bikepedonly = ["3157B", "3157A", "3156", "3156A"]
###############################################################################

# set range to number of 15 minute segments the script should run for. i.e. 40 = 10 hours
# script can be called outside docker container
for x in range(1,10800):
    #emptying result camera dictionary for the next iteration
    camera_dictionary = {}
    recent_crossings = {}
    road_dictionary = {}
    polygon_dictionary_ped = {}
    polygon_dictionary_bike = {}
    final_op=[]
    now = datetime.now()
    # set future to seconds=15*60 for 15 minutes
    future = now + timedelta(seconds=15*60)
    print("x=",x," ",now, future)
    consumer = KafkaConsumer('direction', bootstrap_servers='localhost:9092')
    for message in consumer:
        # if time is still within the 15 minute segment
        if datetime.now() < future:
            # Decode message value from bytes to string
            message_value = message.value.decode('utf-8')
            # Parse JSON data
            data = json.loads(message_value)
            # Process the received JSON data
            # 2025-02-10 new cameras using set
            camSensorId = data['sensor_id']
            classType = data['class']
            cameras.add(camSensorId)

            objectId = data['object_id']
            direction_key = str(data.get('start_direction', '')) + str(data.get('end_direction', ''))
            now_ts = time.time()

            # prune stale dedup entries so a new crossing can be counted again after the cooldown
            for stale_key, stale_ts in list(recent_crossings.items()):
                if now_ts - stale_ts > crossing_cooldown_seconds:
                    del recent_crossings[stale_key]

            dedup_key = (camSensorId, objectId, classType, direction_key)
            if dedup_key in recent_crossings:
                continue
            recent_crossings[dedup_key] = now_ts
            
            for cam in cameras:
                if cam not in camera_dictionary:
                    # if camera not in the output dictionary yet, add it with 0 for all directions
                    print("adding camera ", cam, "to dictionary")
                    #camera_dictionary[cam] = {"NN":0, "NS":0, "NE":0, "NW":0, "SS":0, "SN":0, "SE":0, "SW":0, "EE":0, "EN":0, "ES":0, "EW":0, "WW":0, "WN":0, "WS":0, "WE":0}
                    camera_dictionary[cam] = {"nn":0, "ns":0, "ne":0, "nw":0, "ss":0, "sn":0, "se":0, "sw":0, "ee":0, "en":0, "es":0, "ew":0, "ww":0, "wn":0, "ws":0, "we":0}
                # if the cameras match
                # use data['name'] to access values from the json stream
                if camSensorId == cam:
                    #print("camera match found")
                    if classType == "Person":
                        #print("person or bike found")
                        # if the class is person or bike, add to polygon dictionary
                        if cam not in polygon_dictionary_ped:
                            polygon_dictionary_ped[cam] = {"n-crosswalk":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0},
                                                        "s-crosswalk":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0},
                                                        "e-crosswalk":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0},
                                                        "w-crosswalk":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0},
                                                        "w-sidewalk-n":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0},
                                                        "w-sidewalk-s":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-sidewalk-w":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-sidewalk-e":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-sidewalk-s":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-sidewalk-n":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-sidewalk-e":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-sidewalk-w":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "w-bikelane-wb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "w-bikelane-eb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-bikelane-sb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-bikelane-nb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-bikelane-eb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-bikelane-wb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-bikelane-nb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-bikelane-sb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-lane-1":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-lane-2":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-lane-3":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-lane-4":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-lane-1":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-lane-2":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-lane-3":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-lane-4":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-lane-1":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-lane-2":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-lane-3":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-lane-4":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "w-lane-1":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "w-lane-2":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "w-lane-3":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "w-lane-4":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}}
                        # check for polygons
                        for poly in data['polygons']:
                            print(data['polygons'])
                            if poly in polygon_dictionary_ped[cam]:
                                print("starting polygon update")
                                # increment count for polygons
                                if classType=="bicycle":
                                    polygon_dictionary_ped[cam][poly]["bike-count"] += 1
                                elif classType=="Person":
                                    polygon_dictionary_ped[cam][poly]["ped-count"] += 1
                                #polygon_dictionary_ped[cam][poly]["count"] += 1
                                # check if waiting time is in data
                                if 'waiting_time' in data and poly in data['waiting_time']:
                                    polygon_dictionary_ped[cam][poly]["ped-wait-time"] += data['waiting_time'][poly]
                                # check if crossing time is in data
                                if 'crossing_time' in data and poly in data['crossing_time']:
                                    polygon_dictionary_ped[cam][poly]["ped-cross-time"] += data['crossing_time'][poly]
                                # check violation and increment count for bike and pedestrian lane violations
                                if classType=="bicycle" and data['violation'] == True and poly in data['violation_details']['pedestrian_lane']:
                                    polygon_dictionary_ped[cam][poly]["bike-violation-count"] += 1
                                    #print("violation detected for camera", cam, "object id", data['object_id'], "class", classType, "polygons", data['polygons'])
                                    #print(data['violation_details']['pedestrian_lane'])
                                elif classType=="Person" and data['violation'] == True and poly in data['violation_details']:
                                    polygon_dictionary_ped[cam][poly]["ped-violation-count"] += 1
                                # increment maximum waiting time
                                if 'waiting_time' in data and poly in data['waiting_time']:
                                    if data['waiting_time'][poly] > polygon_dictionary_ped[cam][poly]["ped-wait-time-max"]:
                                        polygon_dictionary_ped[cam][poly]["ped-wait-time-max"] = data['waiting_time'][poly]
                                # increment maximum crossing time
                                if 'crossing_time' in data and poly in data['crossing_time']:
                                    if data['crossing_time'][poly] > polygon_dictionary_ped[cam][poly]["ped-cross-time-max"]:
                                        polygon_dictionary_ped[cam][poly]["ped-cross-time-max"] = data['crossing_time'][poly]
                                print(polygon_dictionary_ped[cam])

                    elif classType == "bicycle":
                        #print("person or bike found")
                        # if the class is person or bike, add to polygon dictionary
                        if cam not in polygon_dictionary_bike:
                            polygon_dictionary_bike[cam] = {"n-crosswalk":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0},
                                                        "s-crosswalk":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0},
                                                        "e-crosswalk":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0},
                                                        "w-crosswalk":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0},
                                                        "w-sidewalk-n":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0},
                                                        "w-sidewalk-s":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-sidewalk-w":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-sidewalk-e":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-sidewalk-s":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-sidewalk-n":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-sidewalk-e":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-sidewalk-w":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "w-bikelane-wb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "w-bikelane-eb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-bikelane-sb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-bikelane-nb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-bikelane-eb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-bikelane-wb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-bikelane-nb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-bikelane-sb":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-lane-1":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-lane-2":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-lane-3":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "n-lane-4":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-lane-1":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-lane-2":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-lane-3":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "s-lane-4":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-lane-1":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-lane-2":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-lane-3":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "e-lane-4":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "w-lane-1":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "w-lane-2":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "w-lane-3":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}, 
                                                        "w-lane-4":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0}}
                        # check for polygons
                        for poly in data['polygons']:
                            print(data['polygons'])
                            if poly in polygon_dictionary_bike[cam]:
                                #print("starting polygon update")
                                # increment count for polygons
                                polygon_dictionary_bike[cam][poly]["bike-count"] += 1
                                # check if waiting time is in data
                                if 'waiting_time' in data and poly in data['waiting_time']:
                                    polygon_dictionary_bike[cam][poly]["ped-wait-time"] += data['waiting_time'][poly]
                                # check if crossing time is in data
                                if 'crossing_time' in data and poly in data['crossing_time']:
                                    polygon_dictionary_bike[cam][poly]["ped-cross-time"] += data['crossing_time'][poly]
                                # check violation and increment count for bike and pedestrian lane violations
                                if classType=="bicycle" and data['violation'] == True and poly in data['violation_details']['pedestrian_lane']:
                                    polygon_dictionary_bike[cam][poly]["bike-violation-count"] += 1
                                    #print("violation detected for camera", cam, "object id", data['object_id'], "class", classType, "polygons", data['polygons'])
                                    #print(data['violation_details']['pedestrian_lane'])
                                # increment maximum waiting time
                                if 'waiting_time' in data and poly in data['waiting_time']:
                                    if data['waiting_time'][poly] > polygon_dictionary_bike[cam][poly]["ped-wait-time-max"]:
                                        polygon_dictionary_bike[cam][poly]["ped-wait-time-max"] = data['waiting_time'][poly]
                                # increment maximum crossing time
                                if 'crossing_time' in data and poly in data['crossing_time']:
                                    if data['crossing_time'][poly] > polygon_dictionary_bike[cam][poly]["ped-cross-time-max"]:
                                        polygon_dictionary_bike[cam][poly]["ped-cross-time-max"] = data['crossing_time'][poly]
                                print(polygon_dictionary_bike[cam])
                    # remove trucks and bus, just keep cars for now. 2027-07-31
                    #elif classType == "car" or classType == "bus" or classType == "truck":
                    elif classType == "car":
                        #print("car, bus or truck found")
                        # check all directions
                        for direc in directions:
                            #print(cam, "direction", direc, str(data['start_direction']+data['end_direction']))
                            # if directions match. str() to handle null values.
                            if str(data['start_direction']) + str(data['end_direction']) == direc:
                                #print('match found')
                                # add start/end road to road dictionary with key as 3029-NS:{start_road_name:, end_road_name:}
                                if str(cam)+"-"+(direc) not in road_dictionary:
                                    road_dictionary[str(cam)+"-"+(direc)] = {"start_road_name":data['start_direction'], "end_road_name":data['end_direction']}
                                # access dictionary and increment direction value
                                #print(camera_dictionary[cam])
                                #print(camera_dictionary[cam][direc])
                                camera_dictionary[cam][direc] += 1

            final_op.append(data)
            #print(len(final_op))
        else:
            print("end time ", datetime.now())
            # close the consumer, otherwise the loop will not go to the next iteration
            consumer.close()


    ##### put logic here to push to producer
    #print(camera_dictionary)
    #json_list = []
    # for each camera
    for key, value in camera_dictionary.items():
        # for each camera that is not bike/ped only
        if str(key) not in bikepedonly:
            print(str(key), ":", value)
            # for each camera direction
            for direc2, direc2_value in value.items():
                if direc2_value > 0:
                    # add road names
                    for key_cam_direc, value_srname_ername in road_dictionary.items():
                        if str(key)+"-"+str(direc2) == key_cam_direc:
                    #val_to_append = key, direc2, direc2_value, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            #val_to_append = {"id":key, "rddir":direc2, "start_direction":str(direc2)[0], "end_direction":str(direc2)[1], "count":direc2_value, "start_road_name":value_srname_ername["start_road_name"], "end_road_name":value_srname_ername["end_road_name"], "time":datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                            val_to_append = {"id":key,
                                                "class":"vehicle",
                                                "polygon":"", 
                                                "ped_count":"", 
                                                "bike_count":"", 
                                                "ped_wait_time":"", 
                                                "ped_cross_time":"", 
                                                "ped_violation_count":"", 
                                                "bike_violation_count":"", 
                                                "ped_wait_time_max":"", 
                                                "ped_cross_time_max":"",
                                                "time":datetime.now().astimezone(est).strftime("%Y-%m-%d %H:%M:%S"),
                                                "rddir":direc2,
                                                "start_direction":str(direc2)[0], 
                                                "end_direction":str(direc2)[1],
                                                "count":direc2_value,
                                                "start_road_name":value_srname_ername["start_road_name"], 
                                                "end_road_name":value_srname_ername["end_road_name"],
                                                "bikepedonly":"false"}
                    #json_list.append(val_to_append)
                            #print(json.dumps(val_to_append))
                            message_value = str(json.dumps(val_to_append)).encode('utf-8')
                            print("sending to topic", topic, "value", message_value)
                            # sending to producer
                            producer.send(topic, value=message_value)
                                #print(json_list)
    print("number of messages:", len(final_op))


    for key, value in polygon_dictionary_ped.items():
        for poly, poly_value in value.items():
            if poly_value["ped-count"] > 0:
                #print("polygon", poly, "ped-count", poly_value["ped-count"], "bike-count", poly_value["bike-count"])
                #val_to_append = {"id":key, "polygon":poly, "ped_count":poly_value["ped-count"], "bike_count":poly_value["bike-count"], "ped_wait_time":poly_value["ped-wait-time"], "ped_cross_time":poly_value["ped-cross-time"], "ped_violation_count":poly_value["ped-violation-count"], "bike_violation_count":poly_value["bike-violation-count"], "ped_wait_time_max":poly_value["ped-wait-time-max"], "ped_cross_time_max":poly_value["ped-cross-time-max"], "time":datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                if str(key) in bikepedonly:
                    val_to_append = {"id":key, 
                                    "class":"ped", 
                                    "polygon":poly, 
                                    "ped_count":poly_value["ped-count"], 
                                    "bike_count":"", 
                                    "ped_wait_time":poly_value["ped-wait-time"], 
                                    "ped_cross_time":poly_value["ped-cross-time"], 
                                    "ped_violation_count":poly_value["ped-violation-count"], 
                                    "bike_violation_count":poly_value["bike-violation-count"], 
                                    "ped_wait_time_max":poly_value["ped-wait-time-max"], 
                                    "ped_cross_time_max":poly_value["ped-cross-time-max"], 
                                    "time":datetime.now().astimezone(est).strftime("%Y-%m-%d %H:%M:%S"),
                                    "rddir":"",
                                    "start_direction":"", 
                                    "end_direction":"",
                                    "count":"",
                                    "start_road_name":"", 
                                    "end_road_name":"",
                                    "bikepedonly":"true"}
                else:
                    val_to_append = {"id":key, 
                                    "class":"ped", 
                                    "polygon":poly, 
                                    "ped_count":poly_value["ped-count"], 
                                    "bike_count":"", 
                                    "ped_wait_time":poly_value["ped-wait-time"], 
                                    "ped_cross_time":poly_value["ped-cross-time"], 
                                    "ped_violation_count":poly_value["ped-violation-count"], 
                                    "bike_violation_count":poly_value["bike-violation-count"], 
                                    "ped_wait_time_max":poly_value["ped-wait-time-max"], 
                                    "ped_cross_time_max":poly_value["ped-cross-time-max"], 
                                    "time":datetime.now().astimezone(est).strftime("%Y-%m-%d %H:%M:%S"),
                                    "rddir":"",
                                    "start_direction":"", 
                                    "end_direction":"",
                                    "count":"",
                                    "start_road_name":"", 
                                    "end_road_name":"",
                                    "bikepedonly":"false"}
                #print(json.dumps(val_to_append))
                message_value = str(json.dumps(val_to_append)).encode('utf-8')
                print("sending to topic", topic, "value", message_value)
                producer.send(topic, value=message_value)

    for key, value in polygon_dictionary_bike.items():
        for poly, poly_value in value.items():
            if poly_value["bike-count"] > 0:
                if str(key) in bikepedonly:
                    val_to_append = {"id":key, 
                                    "class":"bike", 
                                    "polygon":poly, 
                                    "ped_count":"", 
                                    "bike_count":poly_value["bike-count"], 
                                    "ped_wait_time":poly_value["ped-wait-time"], 
                                    "ped_cross_time":poly_value["ped-cross-time"], 
                                    "ped_violation_count":poly_value["ped-violation-count"], 
                                    "bike_violation_count":poly_value["bike-violation-count"], 
                                    "ped_wait_time_max":poly_value["ped-wait-time-max"], 
                                    "ped_cross_time_max":poly_value["ped-cross-time-max"], 
                                    "time":datetime.now().astimezone(est).strftime("%Y-%m-%d %H:%M:%S"),
                                    "rddir":"",
                                    "start_direction":"", 
                                    "end_direction":"",
                                    "count":"",
                                    "start_road_name":"", 
                                    "end_road_name":"",
                                    "bikepedonly":"true"}
                else:
                     val_to_append = {"id":key, 
                                    "class":"bike", 
                                    "polygon":poly, 
                                    "ped_count":"", 
                                    "bike_count":poly_value["bike-count"], 
                                    "ped_wait_time":poly_value["ped-wait-time"], 
                                    "ped_cross_time":poly_value["ped-cross-time"], 
                                    "ped_violation_count":poly_value["ped-violation-count"], 
                                    "bike_violation_count":poly_value["bike-violation-count"], 
                                    "ped_wait_time_max":poly_value["ped-wait-time-max"], 
                                    "ped_cross_time_max":poly_value["ped-cross-time-max"], 
                                    "time":datetime.now().astimezone(est).strftime("%Y-%m-%d %H:%M:%S"),
                                    "rddir":"",
                                    "start_direction":"", 
                                    "end_direction":"",
                                    "count":"",
                                    "start_road_name":"", 
                                    "end_road_name":"",
                                    "bikepedonly":"false"}
                #print(json.dumps(val_to_append))
                message_value = str(json.dumps(val_to_append)).encode('utf-8')
                print("sending to topic", topic, "value", message_value)
                producer.send(topic, value=message_value)



    #encode for kafka
    #message_value = str(final_op).encode('utf-8')
    # producer.send currently fails due to data in wrong format, needs JSON as output?
    #producer.send(topic, value=message_value)
    ##### end producer logic

# probably not necessary to close consumer here but just in case
consumer.close()
producer.close()

print("end script")
