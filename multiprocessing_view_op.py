from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import time
#import sys
import schedule
import multiprocessing
import datetime

global final_op
final_op =[]

# Kafka broker address
global bootstrap_servers
bootstrap_servers = 'localhost:9092'

# Kafka topic to which you want to send the data
global topic
topic = 'directioncount'

# Create a Kafka producer instance
global producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def consumeDir():
    print("in consumeDir function")
    #consumer2= KafkaConsumer('directioncount', bootstrap_servers='localhost:9092')
    consumer = KafkaConsumer('direction', bootstrap_servers='localhost:9092')
    # Continuously listen for incoming messages
    for message in consumer:
        # Decode message value from bytes to string
        message_value = message.value.decode('utf-8')
        # Parse JSON data
        data = json.loads(message_value)
        # Process the received JSON data
        # Process the received JSON data
        #print('Received:')
        #print(json.dumps(data, indent=4))
        global final_op
        final_op.append(data)
        #print(final_op)
            # You can add your own logic here to process the received JSON data
        # Close Kafka consumer
    consumer.close()


def timer_function():
    print('in timer function')
        #print(final_op[0:10])
        #sys.wait(1*60) #or sys.wait
        #u_data["kafka_obj"].send('direction', value=final_op)  
        #send final_op #create message calls or API brokers here, or save to file
        # Send the data to the Kafka topic
        # Data to be sent to the Kafka topic
    global final_op
    data = final_op
    print(len(final_op))
    #print(data)
        # Convert data to bytes (Kafka messages must be bytes)
    message_value = str(data).encode('utf-8')
    producer.send(topic, value=message_value)
    final_op = []

# Schedule the function to run every minute
def scheduler1():
    print("in scheduler function")
    schedule.every(1).minutes.do(timer_function)

    # Run the scheduled tasks
    while True:
        time.sleep(20)
        schedule.run_pending()
        time.sleep(60)  # Sleep for 1 second to avoid high CPU usage

p1 = multiprocessing.Process(target=consumeDir)
p2 = multiprocessing.Process(target=scheduler1)

p1.start()
p2.start()
