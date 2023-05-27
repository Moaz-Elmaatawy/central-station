package com.example.Bitcask;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Demo {

    public static void main(String[] args) throws IOException {
        long start_delay = 5, wait_delay = 60;
        int n_samples = 120;
        int n_stations = 20;

        Writer writer = new Writer(n_samples, n_stations);
        writer.start();
        Reader[] readers = new Reader[10];
        for (int i = 0; i < 10; i++) {
            readers[i] = new Reader(n_stations);
            readers[i].start();
        }
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Compactor comp = new Compactor();
        scheduler.scheduleAtFixedRate(comp, start_delay, wait_delay, TimeUnit.SECONDS);
    }
}
