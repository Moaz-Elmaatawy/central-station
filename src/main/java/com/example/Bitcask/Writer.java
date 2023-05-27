package com.example.Bitcask;

import java.io.IOException;
import java.util.Random;

public class Writer extends Thread {

    private int n_samples;
    private int n_stations;
    private String[] levels;

    public Writer (int n_samples, int n_stations) throws IOException {
        this.n_samples = n_samples;
        this.n_stations = n_stations;
        levels = new String[]{"low", "medium", "high"};
    }

    @Override
    public void run() {
        try {
            BitCaskServer bcs = BitCaskServer.getInstance();
            Random rand = new Random();
            for (int i = 0; i < n_samples; i++) {
                long id = rand.nextInt(n_stations);
                String battery_status = levels[rand.nextInt(3)];
                long status_timestamp = System.currentTimeMillis();
                int humidity = rand.nextInt(101);
                int temperature = rand.nextInt(500);
                int wind_speed = rand.nextInt(3500);
                WeatherStatusSerialized status = new WeatherStatusSerialized(id, battery_status,
                        status_timestamp, humidity, temperature, wind_speed);
                bcs.put(status);
                System.out.println("Writing Done for key: " + id);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
