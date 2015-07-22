FROM java:7-jre

RUN mkdir -p /app
WORKDIR /app

ADD pgobserver.yaml /root/.pgobserver.yaml
ADD target/pgobserver-gatherer-1.?.?-jar-with-dependencies.jar /app/pgobserver-gatherer.jar

EXPOSE 8081

CMD ["java","-jar","/app/pgobserver-gatherer.jar"]
