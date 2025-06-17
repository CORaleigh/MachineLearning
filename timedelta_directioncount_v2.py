 # timedelta_directioncount_new.py
 # This script consumes messages from a Kafka topic named 'direction', 
 # processes the data to count vehicle and pedestrian movements in various directions, 
 # and produces aggregated results back to a Kafka topic named 'directioncount'.
 
""" 
# data is by polygon, in dashboard, counts, average times, crossing times, waiting times, violations
 Todo:    
over 15 minutes:

crossing count
average crosswalk time
minimum crossing time
maximum crossing time

waiting time
person count
average waiting time
min
max       


 Topic: direction
Received:
{
    "id": "3168",
    "object_id": 3,
    "start_road_name": "No / Low Movement 1.1577378275975208",
    "start_direction": "No / Low Movement 1.1577378275975208",
    "end_road_name": "No / Low Movement 1.1577378275975208",
    "end_direction": "No / Low Movement 1.1577378275975208",
    "time": "23/05/2025 19:22:43"
}
-----------------------------------

Topic: direction
Received:
{
    "sensor_id": "3188",
    "object_id": 7323,
    "class": "Person",
    "start_timestamp": "2025-05-23 17:44:40",
    "end_timestamp": "2025-05-23 17:44:45",
    "start_direction": "s",
    "end_direction": "s",
    "polygons": [
        "s-crosswalk"
    ],
    "movement": "U",
    "violation": false,
    "violation_details": "",
    "waiting_time": {},
    "crossing_time": {},
    "near_miss_detection": false,
    "time": "23/05/2025 19:22:43",
    "message_type": "trajectory_data"
}


Topic: direction
Received:
{
    "sensor_id": "3168",
    "object_id": 7576,
    "class": "car",
    "start_timestamp": "2025-05-23 17:46:05",
    "end_timestamp": "2025-05-23 17:46:09",
    "start_direction": "s",
    "end_direction": "n",
    "polygons": [
        "s-crosswalk",
        "n-crosswalk"
    ],
    "movement": "T",
    "violation": false,
    "violation_details": "",
    "waiting_time": "",
    "crossing_time": "",
    "near_miss_detection": false,
    "time": "23/05/2025 19:26:11",
    "message_type": "trajectory_data"
}
-----------------------------------

Topic: direction
Received:
{
    "sensor_id": "3188",
    "object_id": 7587,
    "class": "car",
    "start_timestamp": "2025-05-23 17:46:45",
    "end_timestamp": "2025-05-23 17:46:46",
    "start_direction": "s",
    "end_direction": "s",
    "polygons": [
        "s-crosswalk",
        "s-bikelane-sb"
    ],
    "movement": "U",
    "violation": false,
    "violation_details": "",
    "waiting_time": "",
    "crossing_time": "",
    "near_miss_detection": false,
    "time": "23/05/2025 19:26:12",
    "message_type": "trajectory_data"
}
-----------------------------------

Topic: direction
Received:
{
    "sensor_id": "3188",
    "object_id": 7565,
    "class": "car",
    "start_timestamp": "2025-05-23 17:46:40",
    "end_timestamp": "2025-05-23 17:46:47",
    "start_direction": "s",
    "end_direction": "n",
    "polygons": [
        "s-bikelane-sb",
        "s-crosswalk",
        "w-crosswalk",
        "n-crosswalk"
    ],
    "movement": "T",
    "violation": false,
    "violation_details": "",
    "waiting_time": "",
    "crossing_time": "",
    "near_miss_detection": false,
    "time": "23/05/2025 19:26:12",
    "message_type": "trajectory_data"
}

{
      "_id": "6835157ad43e7a7522bec3f1",
      "sensor_id": "3188",
      "object_id": 223,
      "class": "bicycle",
      "start_timestamp": "2025-05-27 01:29:38",
      "end_timestamp": "2025-05-27 01:29:43",
      "start_direction": "n",
      "end_direction": "n",
      "polygons": [
        "n-crosswalk",
        "n-sidewalk-w"
      ],
      "movement": "U",
      "violation": true,
      "violation_details": {
        "pedestrian_lane": [
          "n-sidewalk-w"
        ]
      },
      "waiting_time": "",
      "crossing_time": "",
      "near_miss_detection": false,
      "time": "27/05/2025 01:29:30",
      "message_type": "trajectory_data",
      "ingested_at": 1748309370.3665698
    }

    {
      "_id": "6835157ad43e7a7522bec3f0",
      "sensor_id": "3188",
      "object_id": 163,
      "class": "Person",
      "start_timestamp": "2025-05-27 01:29:03",
      "end_timestamp": "2025-05-27 01:29:43",
      "start_direction": "n",
      "end_direction": "n",
      "polygons": [
        "n-sidewalk-e",
        "n-crosswalk",
        "n-lane-3",
        "n-sidewalk-w"
      ],
      "movement": "U",
      "violation": true,
      "violation_details": "w-crosswalk",
      "waiting_time": {
        "n-crosswalk": 3
      },
      "crossing_time": {
        "n-crosswalk": 12
      },
      "near_miss_detection": false,
      "time": "27/05/2025 01:29:30",
      "message_type": "trajectory_data",
      "ingested_at": 1748309370.148722
    },

  """

from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import time
import datetime

global final_op
final_op =[]

# open json config file for camera metadata
#with open('cortraffic.json') as f:
with open('/u01/app/CAR_COUNTING_V102/inferencing_pipeline_count/cortraffic.json') as f:
    config_file = json.load(f)

# Set up Kafka consumer
# Kafka broker address
bootstrap_servers = 'localhost:9092'
# Kafka topic to which you want to send the data
topic = 'directioncount'
# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

#consumer2= KafkaConsumer('directioncount', bootstrap_servers='localhost:9092')
consumer = KafkaConsumer('direction', bootstrap_servers='localhost:9092')

from datetime import datetime, timedelta
import time

####camera stuff
cameras = set()
#polygon_dictionary = set()
directions = ["NN", "NS", "NE", "NW", "SS", "SN", "SE", "SW", "EE", "EN", "ES", "EW", "WW", "WN", "WS", "WE"]


# set range to number of 15 minute segments the script should run for. i.e. 40 = 10 hours
# script can be called outside docker container
for x in range(1,10800):
    #emptying result camera dictionary for the next iteration
    camera_dictionary = {}
    road_dictionary = {}
    polygon_dictionary = {}
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
            for cam in cameras:
                if cam not in camera_dictionary:
                    # if camera not in the output dictionary yet, add it with 0 for all directions
                    #camera_dictionary[cam] = {"NN":0, "NS":0, "NE":0, "NW":0, "SS":0, "SN":0, "SE":0, "SW":0, "EE":0, "EN":0, "ES":0, "EW":0, "WW":0, "WN":0, "WS":0, "WE":0}
                    camera_dictionary[cam] = {"nn":0, "ns":0, "ne":0, "nw":0, "ss":0, "sn":0, "se":0, "sw":0, "ee":0, "en":0, "es":0, "ew":0, "ww":0, "wn":0, "ws":0, "we":0}
                # if the cameras match
                # use data['name'] to access values from the json stream
                if camSensorId == cam:
                    if classType == "Person" or classType == "bicycle":
                        #print("person found")
                        # if the class is person or bike, add to polygon dictionary
                        if cam not in polygon_dictionary:
                            polygon_dictionary[cam] = {"n-crosswalk":{"ped-count":0,"bike-count":0, "ped-wait-time":0,"ped-cross-time":0, "ped-violation-count":0, "bike-violation-count":0, "ped-wait-time-max":0, "ped-cross-time-max":0},
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
                            if poly in polygon_dictionary[cam]:
                                # increment count for polygon
                                polygon_dictionary[cam][poly]["count"] += 1
                                # check if waiting time is in data
                                if 'waiting_time' in data and poly in data['waiting_time']:
                                    polygon_dictionary[cam][poly]["wait-time"] += data['waiting_time'][poly]
                                # check if crossing time is in data
                                if 'crossing_time' in data and poly in data['crossing_time']:
                                    polygon_dictionary[cam][poly]["cross-time"] += data['crossing_time'][poly]
                                # check violation and increment count for bike and pedestrian lane violations
                                if classType=="bicycle" and data['violation'] == True and poly in data['violation_details']['pedestrian_lane']:
                                    polygon_dictionary[cam][poly]["bike-violation-count"] += 1
                                    #print("violation detected for camera", cam, "object id", data['object_id'], "class", classType, "polygons", data['polygons'])
                                    #print(data['violation_details']['pedestrian_lane'])
                                elif classType=="Person" and data['violation'] == True and poly in data['violation_details']:
                                    polygon_dictionary[cam][poly]["ped-violation-count"] += 1
                                # increment maximum waiting time
                                if 'waiting_time' in data and poly in data['waiting_time']:
                                    if data['waiting_time'][poly] > polygon_dictionary[cam][poly]["ped-wait-time-max"]:
                                        polygon_dictionary[cam][poly]["ped-wait-time-max"] = data['waiting_time'][poly]
                                # increment maximum crossing time
                                if 'crossing_time' in data and poly in data['crossing_time']:
                                    if data['crossing_time'][poly] > polygon_dictionary[cam][poly]["ped-cross-time-max"]:
                                        polygon_dictionary[cam][poly]["ped-cross-time-max"] = data['crossing_time'][poly]

                    elif classType == "car" or classType == "bus" or classType == "truck":
                    # check all directions
                        for direc in directions:
                            #print(cam, "direction", direc, str(data['start_direction']+data['end_direction']))
                            # if directions match. str() to handle null values.
                            if str(data['start_direction']) + str(data['end_direction']) == direc:
                                #print('match found')
                                # add start/end road to road dictionary with key as 3029-NS:{start_road_name:, end_road_name:}
                                if str(cam)+"-"+(direc) not in road_dictionary:
                                    road_dictionary[str(cam)+"-"+(direc)] = {"start_road_name":data['start_road_name'], "end_road_name":data['end_road_name']}
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
        # for each camera direction
        for direc2, direc2_value in value.items():
            if direc2_value > 0:
                # add road names
                for key_cam_direc, value_srname_ername in road_dictionary.items():
                    if str(key)+"-"+str(direc2) == key_cam_direc:
                #val_to_append = key, direc2, direc2_value, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        val_to_append = {"id":key, "rddir":direc2, "start_direction":str(direc2)[0], "end_direction":str(direc2)[1], "count":direc2_value, "start_road_name":value_srname_ername["start_road_name"], "end_road_name":value_srname_ername["end_road_name"], "time":datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                #json_list.append(val_to_append)
                        print(json.dumps(val_to_append))
                        message_value = str(json.dumps(val_to_append)).encode('utf-8')
                        # don't send yet, wait for data from 
                        producer.send(topic, value=message_value)
                            #print(json_list)
    print("number of messages:", len(final_op))


    for key, value in polygon_dictionary.items():
        for poly, poly_value in value.items():
            if poly_value["ped-count"] > 0 or poly_value["bike-count"] > 0:
                #print("polygon", poly, "ped-count", poly_value["ped-count"], "bike-count", poly_value["bike-count"])
                val_to_append = {"id":key, "polygon":poly, "ped_count":poly_value["ped-count"], "bike_count":poly_value["bike-count"], "ped_wait_time":poly_value["ped-wait-time"], "ped_cross_time":poly_value["ped-cross-time"], "ped_violation_count":poly_value["ped-violation-count"], "bike_violation_count":poly_value["bike-violation-count"], "ped_wait_time_max":poly_value["ped-wait-time-max"], "ped_cross_time_max":poly_value["ped-cross-time-max"], "time":datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                print(json.dumps(val_to_append))
                message_value = str(json.dumps(val_to_append)).encode('utf-8')
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