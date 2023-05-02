package com.example.Writers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.File;
import java.util.Collections;

public class JsonToParquetWriter{

    public static void main(String[] args) {
        String inputPath = "src/input.json";
        String outputPath = "output.parquet";

        // Create a SparkSession object
        SparkSession spark = SparkSession.builder()
                .appName("JsonToParquetConverter")
                .master("local[*]")  // Set the master URL for the local mode
                .getOrCreate();

//        // Read the input JSON file into a DataFrame
//        Dataset<Row> inputDF = spark.read().option("multiline","true").json(inputPath);
//        inputDF.show(10,0);
//        //inputDF.select("station_id","weather.*").show(10,0);
//        // Write the DataFrame to Parquet format
//        inputDF.write().parquet(outputPath);
//
//        // Stop the SparkSession
//        spark.stop();
        //--------------------------------------------------convert Json string directly-----------------------------------------
        String jsonStr = "[\n" +
                "{\"station_id\":1,\"s_no\":256,\"battery_status\":\"medium\",\"status_timestamp\":1683008923,\"weather\":{\"humidity\":86,\"temperature\":21,\"wind_speed\":30}},\n" +
                "{\"station_id\":1,\"s_no\":257,\"battery_status\":\"medium\",\"status_timestamp\":1683008924,\"weather\":{\"humidity\":96,\"temperature\":2,\"wind_speed\":0}},\n" +
                "{\"station_id\":1,\"s_no\":258,\"battery_status\":\"medium\",\"status_timestamp\":1683008925,\"weather\":{\"humidity\":24,\"temperature\":38,\"wind_speed\":3}},\n" +
                "{\"station_id\":1,\"s_no\":259,\"battery_status\":\"medium\",\"status_timestamp\":1683008926,\"weather\":{\"humidity\":57,\"temperature\":23,\"wind_speed\":32}},\n" +
                "{\"station_id\":1,\"s_no\":260,\"battery_status\":\"low\",\"status_timestamp\":1683008927,\"weather\":{\"humidity\":22,\"temperature\":24,\"wind_speed\":14}},\n" +
                "{\"station_id\":1,\"s_no\":261,\"battery_status\":\"high\",\"status_timestamp\":1683008928,\"weather\":{\"humidity\":31,\"temperature\":41,\"wind_speed\":3}},\n" +
                "{\"station_id\":1,\"s_no\":262,\"battery_status\":\"high\",\"status_timestamp\":1683008929,\"weather\":{\"humidity\":10,\"temperature\":75,\"wind_speed\":9}},\n" +
                "{\"station_id\":1,\"s_no\":263,\"battery_status\":\"medium\",\"status_timestamp\":1683008930,\"weather\":{\"humidity\":36,\"temperature\":70,\"wind_speed\":38}},\n" +
                "{\"station_id\":1,\"s_no\":265,\"battery_status\":\"low\",\"status_timestamp\":1683008931,\"weather\":{\"humidity\":30,\"temperature\":67,\"wind_speed\":36}},\n" +
                "{\"station_id\":1,\"s_no\":266,\"battery_status\":\"medium\",\"status_timestamp\":1683008932,\"weather\":{\"humidity\":30,\"temperature\":26,\"wind_speed\":1}}\n" +
                "]";

        // Create a SparkSession object
        spark = SparkSession.builder()
                .appName("ReadJsonArrayToDataFrame")
                .master("local[*]")  // Set the master URL for the local mode
                .getOrCreate();
        Seq<String> s=JavaConverters.asScalaBuffer(Collections.singletonList(jsonStr)).toSeq();
        // Read the JSON array into a DataFrame
        Dataset<Row> ds = spark.read().json(spark.createDataset(s, Encoders.STRING()));

        // Show the DataFrame contents
        ds.show();

        ds.write().parquet(outputPath);

        // Stop the SparkSession
        spark.stop();
        // Stop the SparkSession
    }
    public static void writeParquet(String jsonStr,String outputPath,SparkSession spark) {
        // Create a SparkSession object
        File directory = new File(outputPath.substring(0,outputPath.lastIndexOf("/")));
        if (! directory.exists()){
            directory.mkdirs();
        }
        Seq<String> s=JavaConverters.asScalaBuffer(Collections.singletonList(jsonStr)).toSeq();
        // Read the JSON array into a DataFrame
        Dataset<Row> ds = spark.read().json(spark.createDataset(s, Encoders.STRING()));

        // Show the DataFrame contents
        ds.show();

        ds.write().parquet(outputPath);

    }


}