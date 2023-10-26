from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import time
import datetime

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

from datetime import datetime, timedelta
import time

# set range to number of 15 minute segments the script should run for. i.e. 40 = 10 hours
# or set for two hour segments and call the script on schedule via cron
# script can be called outside docker container
for x in range(1,40):
    final_op=[]
    now = datetime.now()
    # set future to seconds=15*60 for 15 minutes
    future = now + timedelta(seconds=10)
    print("x=",x," ",now, future)
    consumer = KafkaConsumer('direction', bootstrap_servers='localhost:9092')
    for message in consumer:
        #print(datetime.now(), future)
        if datetime.now() < future:
            # Decode message value from bytes to string
            message_value = message.value.decode('utf-8')
            # Parse JSON data
            data = json.loads(message_value)
            # Process the received JSON data
            #print(json.dumps(data, indent=4))
            ######### Add logic here for turning movement counts
            final_op.append(data)
            #print(len(final_op))
        else:
            print("end time ", datetime.now())
            # close the consumer, otherwise the loop will not go to the next iteration
            consumer.close()
    # put logic here to push to producer        
    print("number of cars:", len(final_op))
    message_value = str(final_op).encode('utf-8')
    # producer.send currently fails due to data in wrong format, needs JSON as output?
    producer.send(topic, value=message_value)
# probably not necessary to close consumer here but just in case
consumer.close()
producer.close()
