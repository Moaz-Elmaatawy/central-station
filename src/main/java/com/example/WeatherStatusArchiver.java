package com.example;

import com.example.Writers.JsonToParquetWriter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.spark.sql.SparkSession;

import java.util.Map.Entry;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class WeatherStatusArchiver {
    private final int BATCH_SIZE=60;
    int counter=0;
    HashMap<Long,ArrayList<String>>stationData;
    SparkSession spark;

    public WeatherStatusArchiver(){
        stationData=new HashMap<>();
        spark = SparkSession.builder()
                .appName("ReadJsonArrayToDataFrame")
                .master("local[*]")  // Set the master URL for the local mode
                .getOrCreate();
    }

    public void archiveRecord(ConsumerRecords<Long, String> records){

        for (ConsumerRecord<Long, String> record : records) {
            System.out.println("Received message: " + record.value());
            Long stationID=record.key();
            System.out.println("Station ID is ==== "+stationID);
            if(!stationData.containsKey(stationID)){
                stationData.put(stationID,new ArrayList<String>());
            }
            stationData.get(stationID).add(record.value());
            counter++;
            System.out.println("counter === "+counter);
            if(counter >= BATCH_SIZE){
                writeDataInParquetFiles(stationData,spark);
                stationData.clear();
                counter=0;
            }
        }
    }

    public String getCurrentDate(long time){
        Timestamp ts=new Timestamp(time);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(ts);
    }
    public String getCurrentTIme(long time){
        Timestamp ts=new Timestamp(time);
        DateFormat timeFormat = new SimpleDateFormat("hh-mm-ss");
        return timeFormat.format(ts);
    }
    public void writeDataInParquetFiles(HashMap<Long,ArrayList<String>>data,SparkSession sparkSession){
        Set<Entry<Long, ArrayList<String>>> entrySet = data.entrySet();
        for (Entry<Long, ArrayList<String>> entry : entrySet) {
            long ts=Long.parseLong(entry.getValue().get(0).toString().split(",")[3].split(":")[1]);
            String outputPath="parquet_files/stationID_"+entry.getKey()+"/"+getCurrentDate(ts)+"/"+getCurrentTIme(ts);
            JsonToParquetWriter.writeParquet(entry.getValue(),outputPath,sparkSession);
        }
        data.clear();
    }
}
