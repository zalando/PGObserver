FROM dockerfile/java:oracle-java7

RUN mkdir -p /app
WORKDIR /app

ADD target/pgobserver-gatherer-1.0.4-jar-with-dependencies.jar /app/pgobserver-gatherer.jar

EXPOSE 8081

CMD ["java","-jar","/app/pgobserver-gatherer.jar"]