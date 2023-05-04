package com.example;

import com.example.Writers.JsonToParquetWriter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.SparkSession;
import org.json4s.jackson.Json;

import java.lang.reflect.Type;
import java.util.Map.Entry;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

public class WeatherDataConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "weather-data-consumer");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        SparkSession spark = SparkSession.builder()
                .appName("ReadJsonArrayToDataFrame")
                .master("local[*]")  // Set the master URL for the local mode
                .getOrCreate();
        int batchSize=10000;
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("weather_data"));
        int counter=0;
        HashMap<Integer,StringBuilder>stationsData=new HashMap<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received message: " + record.value());
                int stationID=Integer.parseInt(record.value().split(",")[0].split(":")[1]);
                System.out.println("Station ID is ==== "+stationID);
                if(stationsData.containsKey(stationID)){
                    stationsData.put(stationID, stationsData.get(stationID).append(",\n").append(record.value()));
                }else {
                    stationsData.put(stationID,new StringBuilder(record.value()));
                }
                counter++;
                System.out.println("counter === "+counter);
                if(counter>=batchSize){
                    writeDataInParquetFiles(deepCopy(stationsData),spark);
                    stationsData.clear();
                    counter=0;
                }
            }
        }
    }
    public static String getCurrentDate(){
        Timestamp ts=new Timestamp(System.currentTimeMillis());
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(ts);
    }
    public static String getCurrentTIme(){
        Timestamp ts=new Timestamp(System.currentTimeMillis());
        DateFormat timeFormat = new SimpleDateFormat("hh-mm-ss-SSS");
        return timeFormat.format(ts);
    }
    public static void writeDataInParquetFiles(HashMap<Integer,StringBuilder>data,SparkSession sparkSession){
        Set<Entry<Integer, StringBuilder> > entrySet = data.entrySet();
        for (Entry<Integer, StringBuilder> entry : entrySet) {
            String outputPath="stationID_"+entry.getKey()+"/"+getCurrentDate()+"/"+getCurrentTIme()+".parquet";
            String jsonRecords="[\n"+entry.getValue()+"\n]";
            System.out.println(jsonRecords);
            JsonToParquetWriter.writeParquet(jsonRecords,outputPath,sparkSession);
        }
        data.clear();
    }
    public static HashMap<Integer,StringBuilder>deepCopy(HashMap<Integer,StringBuilder>clonedHash){
        Gson gson = new Gson();
        String jsonString = gson.toJson(clonedHash);
        Type type = new TypeToken<HashMap<Integer, StringBuilder>>(){}.getType();
        HashMap<Integer, StringBuilder> deepClonedMap = gson.fromJson(jsonString, type);
        System.out.println(clonedHash==deepClonedMap);
        return deepClonedMap;
    }


}
