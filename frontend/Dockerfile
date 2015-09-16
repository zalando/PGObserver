FROM orchardup/python:2.7

RUN apt-get update && apt-get -y install python-dev python-psycopg2 libpq-dev libssl-dev



RUN mkdir -p /app
WORKDIR /app
ADD pgobserver.yaml /app/pgobserver.yaml
ADD requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

ADD run.sh /app/run.sh
ADD src /app/src

EXPOSE 8080

ENTRYPOINT ["python", "/app/src/web.py"]
