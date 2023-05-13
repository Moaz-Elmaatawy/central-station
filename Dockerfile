FROM maven:3.6.0-jdk-11-slim

WORKDIR /Central-Station

COPY src/ /Central-Station/src/
COPY pom.xml /Central-Station

RUN mvn clean package

ENTRYPOINT [ "mvn", "exec:java","-Dexec.mainClass=com.example.WeatherDataConsumer"]