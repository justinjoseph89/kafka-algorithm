FROM openjdk:8
ARG application_name
ENV APPLICATION_JAR="${application_name}"
ENV CONFIG="/config/application.yaml"

COPY ./target/${APPLICATION_JAR}.jar app/

WORKDIR /app

CMD java -jar ./${APPLICATION_JAR}.jar file:${CONFIG}