#!/usr/bin/env python
import pika, sys, os, json, io
from minio import Minio
from minio.error import S3Error
import zipfile

host = "minio-mike-mcnamee.flows-dev-cluster-7c309b11fc78649918d0c8b91bcb5925-0000.eu-gb.containers.appdomain.cloud:80"
host = "52.87.255.213:9000"
def process_file(name):
    try:
        # Create a client with the MinIO server playground, its access key
        # and secret key.
        print(f'Processing file {name}')
        client = Minio(
            host,
            access_key="apricot",
            secret_key="apricotapricot",
#            access_key="miniouser",
#            secret_key="miniopassword",
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
        
        new_code = True
        if new_code == True:
            in_memory_zip = io.BytesIO(response.data)
            zf = zipfile.ZipFile(in_memory_zip, "a", zipfile.ZIP_DEFLATED, False)
            list = zf.namelist()

            for fileName in list:
                zf.extract(fileName, path='data')
                print(f'Extracted {fileName}')
                result = client.fput_object("unpacked",fileName,fileName,)
                return fileName
            
        else:
            print('Response=>',response)
            with open('/tmp/'+name, 'wb') as file_data:
                for d in response.stream(32*1024):
                    file_data.write(d)
        
            with zipfile.ZipFile('/tmp/'+name, 'r') as zip_ref:
                zip_ref.extractall("/tmp")
                list = zip_ref.namelist()
                print(f"list ==>{list}")
                #result = client.put_object("unpacked", list, thefile, len(thefile),"binary/octet-stream")
                result = client.fput_object("unpacked", list[0],'/tmp/'+name,)
                print('After object put')
                return list[0]

    except Exception as e:
        print('Exception {e}',e)
    
def main():
    process_file("TestZip.zip")


if __name__ == '__main__':
    try:
        main()
    
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
