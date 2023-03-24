#!/usr/bin/env python
import pika

# note that in the openshift cluster we can simply use the service name 
# (rabbitmq) to connect to the broker
connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='hello')

channel.basic_publish(exchange='',
                      routing_key='hello',
                      body='Hello World!')
print(" [x] Sent 'Hello World!")

connection.close()
