FROM orchardup/python:2.7

RUN mkdir -p /app
WORKDIR /app
ADD requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

ADD run.sh /app/run.sh
ADD src /app/src

EXPOSE 8080

CMD ["python","/app/src/web.py", "-c", "/app/web.conf"]