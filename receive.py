#!/usr/bin/env python
import pika, sys, os, json, io
from minio import Minio
from minio.error import S3Error
import zipfile

host = "minio-mike-mcnamee.flows-dev-cluster-7c309b11fc78649918d0c8b91bcb5925-0000.eu-gb.containers.appdomain.cloud:80"

def process_file(name):
    try:
        # Create a client with the MinIO server playground, its access key
        # and secret key.
        print(f'Processing file {name}')
        client = Minio(
            host,
            access_key="miniouser",
            secret_key="miniopassword",
            secure=False
        )

        print(f'Client created')

        found = client.bucket_exists("input")
        if not found:
            print("Bucket 'input' does not exist")
        else:
            print("Bucket 'input' already exists")
    
        print(f'Getting object')
        response = client.get_object(
                bucket_name = 'input',
                object_name=name
            )
        print(f'Back from get')
        print('Response=>',response)
        #with open('/tmp/'+name, 'wb') as file_data:
        #    for d in response.stream(32*1024):
        #        file_data.write(d)
        
        #with zipfile.ZipFile('/tmp/'+name, 'r') as zip_ref:
        #    zip_ref.extractall("/tmp")
        with zipfile.ZipFile(io.BytesIO(response)) as thezip:
            for zipinfo in thezip.infolist():
                with thezip.open(zipinfo) as thefile:
                   print(f'File from zip {zipinfo.filename}')
                   result = client.put_object("unpacked", zipinfo.filename, thefile, len(thefile))
                   return zipinfo.filename, thefile

    except Exception as e:
        print('Exception {e}',e)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='unpacker-queue')

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        print(" [x] Received props %r" % properties)
        data = json.loads(body)
        print(f' Data==>{data}')
        tokens = data["Key"].split('/')
        name, thefile = process_file(tokens[1])
        ch.basic_publish(amqp.Message(name), routing_key='formatter-queue')
    
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
