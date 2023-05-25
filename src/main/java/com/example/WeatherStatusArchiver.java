package com.example;

import com.example.Writers.JsonToParquetWriter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.spark.sql.SparkSession;

import java.util.Map.Entry;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Set;

public class WeatherStatusArchiver {
    private final int BATCH_SIZE=10000;
    int counter=0;
    HashMap<Integer,StringBuilder>stationData;
    SparkSession spark;

    public WeatherStatusArchiver(){
        stationData=new HashMap<>();
        spark = SparkSession.builder()
                .appName("ReadJsonArrayToDataFrame")
                .master("local[*]")  // Set the master URL for the local mode
                .getOrCreate();
    }

    public void archiveRecord(ConsumerRecords<String, String> records){

        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Received message: " + record.value());
            int stationID=Integer.parseInt(record.value().split(",")[0].split(":")[1]);
            System.out.println("Station ID is ==== "+stationID);
            if(stationData.containsKey(stationID)){
                stationData.put(stationID, stationData.get(stationID).append(",\n").append(record.value()));
            }else {
                stationData.put(stationID,new StringBuilder(record.value()));
            }
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
        DateFormat timeFormat = new SimpleDateFormat("hh-mm-ss-SSS");
        return timeFormat.format(ts);
    }
    public void writeDataInParquetFiles(HashMap<Integer,StringBuilder>data,SparkSession sparkSession){
        Set<Entry<Integer, StringBuilder> > entrySet = data.entrySet();
        for (Entry<Integer, StringBuilder> entry : entrySet) {
            long ts=Long.parseLong(entry.getValue().toString().split(",")[3].split(":")[1]);
            String outputPath="parquet_files/stationID_"+entry.getKey()+"/"+getCurrentDate(ts)+"/"+getCurrentTIme(ts);
            String jsonRecords="[\n"+entry.getValue()+"\n]";
            System.out.println(jsonRecords);
            JsonToParquetWriter.writeParquet(jsonRecords,outputPath,sparkSession);
        }
        data.clear();
    }
}
