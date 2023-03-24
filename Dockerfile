FROM python:3.9-alpine
RUN apk update && apk add --no-cache curl vim bash iputils

WORKDIR /usr/src/app

COPY *.py ./
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN chmod 0777 -R /usr/src/app
RUN pip --version 
RUN python --version

CMD python3 sleep.py
