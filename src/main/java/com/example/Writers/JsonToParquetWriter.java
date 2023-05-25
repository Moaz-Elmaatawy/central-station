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