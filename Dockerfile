FROM openjdk:8-jre-slim

RUN apt-get -y update && apt-get install -y gettext-base
ENV APPLICATION_NAME=kafka-algorithm

ENV CONFIG="/config/application.yaml"

RUN ls

COPY ./target/${APPLICATION_NAME}-jar-with-dependencies.jar /app/${APPLICATION_NAME}.jar
COPY ./application.yaml /config/application.yaml

WORKDIR /app

RUN ls

CMD java -jar /app/${APPLICATION_NAME}.jar ${CONFIG} -Xms500m -Xmx1g