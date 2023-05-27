package com.example.Bitcask;

import java.io.IOException;
import java.util.Random;

public class Reader extends Thread {

    private final int n_stations;

    public Reader(int n_stations) {
        this.n_stations = n_stations;
    }

    @Override
    public void run() {
        try {
            BitCaskServer bcs = BitCaskServer.getInstance();
            Random rand = new Random();
            long key = rand.nextInt(n_stations);
            ValueSerialized val = bcs.get(key);
            if (val != null)
                System.out.println("Reading Done for key: " + key);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
