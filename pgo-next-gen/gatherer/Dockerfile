# List of stups/python versions: https://registry.opensource.zalan.do/teams/stups/artifacts/python/tags
# Code - https://github.com/zalando/docker-python
FROM registry.opensource.zalan.do/stups/python:3.5.1-17

RUN apt-get install -y python3-psycopg2
#RUN pip3 install pgobserver_gatherer

RUN mkdir pgobserver_gatherer

COPY pgobserver_gatherer  ~/pgobserver_gatherer
COPY requirements.txt pgobserver_gatherer.example.yaml  ~/

WORKDIR ~/
RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "-m", "pgobserver_gatherer"]
