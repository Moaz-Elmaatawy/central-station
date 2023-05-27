FROM maven:3.8.6-openjdk-11-slim

WORKDIR /Central-Station

COPY src/ /Central-Station/src/
COPY pom.xml /Central-Station
COPY Elasticsearch-loader/ /Central-Station/Elasticsearch-loader/
COPY entrypoint.sh /Central-Station

RUN apt-get update && apt-get install -y python3.6 python3-pip
RUN pip3 install elasticsearch pandas fastparquet
RUN mkdir parquet_files

RUN mvn clean package

ENTRYPOINT ["/bin/bash", "entrypoint.sh"]