FROM ubuntu:latest

RUN apt-get update && apt-get install -y wget

# http://grafana.org/download/builds.html
# taking the latest "stable" by default
RUN wget -q -O - https://api.github.com/repos/grafana/grafana/tags | grep -Eo '[0-9\.]+' | head -1 > grafana_ver.txt
RUN wget -q -O grafana.tar.gz https://grafanarel.s3.amazonaws.com/builds/grafana-$(cat grafana_ver.txt).linux-x64.tar.gz

RUN mkdir grafana && tar xf grafana.tar.gz -C grafana --strip-components 1
RUN chmod -R 777 grafana

COPY launch_wrapper.py .

EXPOSE 3000

ENTRYPOINT ["python3", "launch_wrapper.py"]
