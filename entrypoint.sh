#!/bin/bash
exec mvn exec:java -Dexec.mainClass=com.example.WeatherDataConsumer &
python3 Elasticsearch-loader/ElasticsearchLoader.py 
