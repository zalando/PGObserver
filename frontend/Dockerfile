# using Ubuntu 12.04 here, 14.04 has python3 as default already
FROM ubuntu:precise

RUN apt-get update && apt-get -y install python-pip python-psycopg2

RUN mkdir -p /app

# needed only to override "features" section. DB connect data can be also fed over ENV vars, see --help
COPY pgobserver_frontend.example.yaml /app/pgobserver.yaml

COPY requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

COPY run.sh /app/run.sh
COPY src /app/src/

EXPOSE 8080

ENTRYPOINT ["python", "/app/src/web.py"]
# by default use the config file - S3 override possible with PGOBS_CONFIG_S3_BUCKET env. var
CMD ["-c", "/app/pgobserver.yaml"]
