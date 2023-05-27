package com.example.Bitcask;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class BitCaskServer {

    private static BitCaskServer instance;
    private String path_active;
    private RandomAccessFile file_active;
    private int counter;

    public static synchronized BitCaskServer getInstance() throws IOException {
        if (instance == null) {
            synchronized(BitCaskServer.class) {
                if (instance == null)
                    instance = new BitCaskServer();
            }
        }
        return instance;
    }

    private BitCaskServer() throws IOException {
        createDir(Globals.MAIN_DIR);
        createDir(Globals.DATA_DIR);
        counter = 1;
        File[] files = new File(Globals.DATA_DIR).listFiles();
        assert files != null;
        for (File file : files)
            counter = Math.max(counter, Utility.extractId(file.getName()));
        if (files.length != 0 && Globals.mainTable.isEmpty())
            recover(files);
        path_active = Globals.DATA_DIR + "file" + counter;
        file_active = new RandomAccessFile(path_active, "rw");
    }

    public boolean put(WeatherStatusSerialized status) throws IOException {
        long len = file_active.length();
        if (len >= Globals.sizeThresh) {
            counter++;
            path_active = Globals.DATA_DIR + "file" + counter;
            file_active.close();
            file_active = new RandomAccessFile(path_active, "rw");
        }
        long offset = file_active.length();
        byte[] serial_data = Serializer.serialize(status);
        long timestamp = Utility.write(file_active, offset, serial_data);
        Globals.mainTable.put(status.getKey(), new ValueTable(counter, offset, timestamp));
        return true;
    }

    public ValueSerialized get(Long key) throws IOException {
        ValueTable kb_main = Globals.mainTable.get(key);
        ValueTable kb_hint = Globals.hintTable.get(key);
        ValueTable kb;
        if (kb_main == null && kb_hint == null) {
            System.out.println("Warning: Key does not exist");
            return null;
        }
        else if (kb_main != null && kb_hint == null)
            kb = kb_main;
        else if (kb_main == null)
            kb = kb_hint;
        else {
            if (kb_main.getTimestamp() > kb_hint.getTimestamp())
                kb = kb_main;
            else
                kb = kb_hint;
        }
        String path = (kb == kb_main) ? Globals.DATA_DIR + "file" + kb_main.getFileId() :
                Globals.MAIN_DIR + "merge";
        RandomAccessFile raf = new RandomAccessFile(path, "rw");
        byte[] record = new byte[Globals.record_size];
        raf.seek(kb.getOffset());
        raf.skipBytes(8);
        int bytes_read = raf.read(record);
        raf.close();
        if (bytes_read == -1) {
            System.out.println("Error: No Bytes Read for this key");
            return null;
        }
        WeatherStatusSerialized status = Serializer.deserialize(record);
        if (!status.getKey().equals(key)) {
            System.out.println("Error: Wrong key, Occurred during reading");
            return null;
        }
        return status.getVal();
    }

    private void recover(File[] files) throws IOException {
        String[] paths = Arrays.stream(files).map(File::getName).
                sorted(Comparator.comparingInt(path -> Utility.extractId((String) path)).reversed()).
                toArray(String[]::new);
        long read_pivot = 0;
        for (String path : paths) {
            RandomAccessFile read_raf = new RandomAccessFile(Globals.DATA_DIR + path, "rw");
            read_pivot = read_raf.length();
            while (read_pivot > 0) {
                read_pivot -= Globals.step;
                if (read_pivot < 0)
                    read_pivot = 0;
                byte[] record = new byte[Globals.record_size];
                read_raf.seek(read_pivot);
                long timestamp = read_raf.readLong();
                int bytes_read = read_raf.read(record);
                if (bytes_read == -1) {
                    System.out.println("Error: No Bytes Read for this key");
                    return;
                }
                WeatherStatusSerialized status = Serializer.deserialize(record);
                if (Globals.mainTable.get(status.getKey()) != null)
                    continue;
                Globals.mainTable.put(status.getKey(),
                        new ValueTable(Utility.extractId(path), read_pivot, timestamp));
            }
            read_raf.close();
        }
        File hintFile = new File(Globals.MAIN_DIR + "hint");
        if (hintFile.exists()) {
            RandomAccessFile read_raf = new RandomAccessFile(hintFile, "rw");
            read_pivot = 0;
            read_raf.seek(read_pivot);
            while (read_pivot < read_raf.length()) {
                long key = read_raf.readLong();
                long offset = read_raf.readLong();
                long timestamp = read_raf.readLong();
                Globals.hintTable.put(key, new ValueTable(0, offset, timestamp));
                read_pivot += Globals.step;
            }
            read_raf.close();
        }
    }

    private void createDir(String path) {
        File directory = new File(path);
        if (!directory.exists()) {
            if (directory.mkdir())
                System.out.println("Directory created successfully!");
            else
                System.out.println("Failed to create directory!");
        }
        else
            System.out.println("Directory already exists!");
    }
}
