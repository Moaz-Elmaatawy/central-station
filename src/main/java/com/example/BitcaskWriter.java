package com.example;

import com.example.Bitcask.BitCaskServer;
import com.example.Bitcask.Compactor;
import com.example.Bitcask.WeatherStatusSerialized;
import com.example.Bitcask.Wrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BitcaskWriter {

    private final Wrapper wrapper;
    private final BitCaskServer bcs;
    private final ScheduledExecutorService scheduler;
    private final Compactor comp;

    private static final long START_DELAY = 10;
    private static final long WAIT_DELAY = 60;

    public BitcaskWriter() throws IOException {
        wrapper = new Wrapper();
        bcs = BitCaskServer.getInstance();
        scheduler = Executors.newSingleThreadScheduledExecutor();
        comp = new Compactor();
        scheduler.scheduleAtFixedRate(comp, START_DELAY, WAIT_DELAY, TimeUnit.SECONDS);
    }

    public void write(ConsumerRecords<Long, String> records) throws IOException {
        for (ConsumerRecord<Long, String> record : records)
            bcs.put(wrapper.convert(record.value()));
    }
}
