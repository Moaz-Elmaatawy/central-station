package com.example.Writers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.collection.JavaConverters;
import java.io.File;
import java.util.ArrayList;

public class JsonToParquetWriter{

    public static void writeParquet(ArrayList<String> jsonArr,String outputPath,SparkSession spark) {
        // Create a SparkSession object
        File directory = new File(outputPath.substring(0,outputPath.lastIndexOf("/")));
        if (! directory.exists()){
            directory.mkdirs();
        }

        scala.collection.Seq<String> seq = JavaConverters.asScalaIteratorConverter(jsonArr.iterator()).asScala().toSeq();

        // Create a Dataset from the Seq
        Dataset<Row> ds = spark.createDataset(seq, Encoders.STRING()).toDF();
        ds.coalesce(1).write().parquet(outputPath);

    }


}