 #
 #
 
""" 
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
        if datetime.now() < future:
            # Decode message value from bytes to string
            message_value = message.value.decode('utf-8')
            # Parse JSON data
            data = json.loads(message_value)
            # Process the received JSON data
            # 2025-02-10 new cameras using set
            camSensorId = data['sensor_id']
            cameras.add(camSensorId)
            for cam in cameras:
                if cam not in camera_dictionary:
                    # if camera not in the output dictionary yet, add it with 0 for all directions
                    camera_dictionary[cam] = {"NN":0, "NS":0, "NE":0, "NW":0, "SS":0, "SN":0, "SE":0, "SW":0, "EE":0, "EN":0, "ES":0, "EW":0, "WW":0, "WN":0, "WS":0, "WE":0}
                    # todo: initiate polygon dictionary for sidewalks and crosswalks
                # if the cameras match
                # use data['name'] to access values from the json stream
                if camSensorId == cam:
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
    for key, value in camera_dictionary.items():
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
                        producer.send(topic, value=message_value)
                            #print(json_list)
    print("number of messages:", len(final_op))
    #encode for kafka
    #message_value = str(final_op).encode('utf-8')
    # producer.send currently fails due to data in wrong format, needs JSON as output?
    #producer.send(topic, value=message_value)
    ##### end producer logic

# probably not necessary to close consumer here but just in case
consumer.close()
producer.close()

print("end script")