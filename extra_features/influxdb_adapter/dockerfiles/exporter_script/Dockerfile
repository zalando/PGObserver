FROM ubuntu:latest

RUN apt-get update && apt-get install -y git libpython2.7-dev libpq-dev python-psycopg2 python-pip

RUN git clone -q https://github.com/zalando/PGObserver.git

WORKDIR PGObserver/extra_features/influxdb_adapter

RUN pip install -r requirements.txt

ENTRYPOINT ["python2", "export_to_influxdb.py", "-v"]
