#!/usr/bin/env python
import pika, sys, os, json
from minio import Minio
from minio.error import S3Error

def process_file(name):
    try:
        # Create a client with the MinIO server playground, its access key
        # and secret key.
        printf(f'Processing file {name}')
        client = Minio(
            "http://minio-mike-mcnamee.flows-dev-cluster-7c309b11fc78649918d0c8b91bcb5925-0000.eu-gb.containers.appdomain.cloud",
            access_key="miniouser",
            secret_key="miniopassword",
        )

        # Make 'asiatrip' bucket if not exist.
        found = client.bucket_exists("input")
        if not found:
            print("Bucket 'input' does not exist")
        else:
            print("Bucket 'input' already exists")
    
        response = client.get_object(
                bucket_name = 'input',
                object_name= name
            )

    except Exception as e:
        print('Exception {e}')

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='unpacker-queue')

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        print(" [x] Received props %r" % properties)
        data = json.loads(body)
        process_file(body.key)
    
    channel.basic_consume(queue='unpacker-queue',
                      auto_ack=True,
                      on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
