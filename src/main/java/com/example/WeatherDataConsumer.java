package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class WeatherDataConsumer {

    private static final int POLLING_TIME = 1000;

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_HOST"));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "weather-data-consumer");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        BitcaskWriter writer = new BitcaskWriter();
        WeatherStatusArchiver archiver = new WeatherStatusArchiver();
        
        try (KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("weather_data"));
            
            while (true) {
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(POLLING_TIME));
                writer.write(records);
                archiver.archiveRecord(records);
            }
        
        }
        catch(Exception e){
            System.out.println(e);
        }
    }
}
