package com.example.Bitcask;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;


public class Compactor extends Thread {

    private final File mergeFile, hintFile, tempMergeFile, tempHintFile;
    private Map<Long, ValueTable> mergeTable;

    public Compactor() {
        mergeFile = new File(Globals.MAIN_DIR + "merge");
        hintFile = new File(Globals.MAIN_DIR + "hint");
        tempMergeFile = new File(Globals.MAIN_DIR + "temp_merge");
        tempHintFile = new File(Globals.MAIN_DIR + "temp_hint");
    }

    @Override
    public void run() {
        File dir = new File(Globals.DATA_DIR);
        if (!dir.exists())
            return;
        File[] files = dir.listFiles();
        assert files != null;
        if (files.length <= Globals.filesThresh)
            return;
        String[] paths = Arrays.stream(files).map(File::getName).
                sorted(Comparator.comparingInt(path -> Utility.extractId((String) path)).reversed()).
                skip(1).toArray(String[]::new);
        boolean flag = !mergeFile.exists() || mergeFile.length() == 0;
        try {
            mergeTable = new HashMap<>();
            processDataFiles(paths, flag);
            writeHintTable(flag);
            Utility.replace(mergeFile, hintFile, tempMergeFile,
                    tempHintFile, mergeTable, flag);
            deleteFiles(paths);
            System.out.println("Compaction Done");
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processDataFiles(String[] paths, boolean flag) throws IOException {
        long read_pivot = 0, write_pivot = 0;
        RandomAccessFile write_raf;
        if (flag)
            write_raf = new RandomAccessFile(mergeFile, "rw");
        else
            write_raf = new RandomAccessFile(tempMergeFile, "rw");
        for (String path : paths) {
            RandomAccessFile read_raf = new RandomAccessFile(Globals.DATA_DIR + path, "rw");
            read_pivot = read_raf.length();
            while (read_pivot > 0) {
                read_pivot -= Globals.step;
                if (read_pivot < 0)
                    read_pivot = 0;
                byte[] record = new byte[Globals.record_size];
                read_raf.seek(read_pivot);
                read_raf.skipBytes(8);
                int bytes_read = read_raf.read(record);
                if (bytes_read == -1) {
                    System.out.println("Error: Bytes Read do not match record_size");
                    return;
                }
                WeatherStatusSerialized status = Serializer.deserialize(record);
                if (mergeTable.get(status.getKey()) != null)
                    continue;
                write_raf.seek(write_pivot);
                long timestamp = Utility.write(write_raf, write_pivot, record);
                mergeTable.put(status.getKey(),
                        new ValueTable(0, write_pivot, timestamp));
                write_pivot += Globals.step;
            }
            read_raf.close();
        }
        write_raf.close();
    }
    
    private void writeHintTable(boolean flag) throws IOException {
        RandomAccessFile write_raf;
        if (flag)
            write_raf = new RandomAccessFile(hintFile, "rw");
        else
            write_raf = new RandomAccessFile(tempHintFile, "rw");
        write_raf.seek(0);
        for (Map.Entry<Long, ValueTable> e : mergeTable.entrySet()) {
            write_raf.writeLong(e.getKey());
            write_raf.writeLong(e.getValue().getOffset());
            write_raf.writeLong(e.getValue().getTimestamp());
        }
        write_raf.close();
    }

    private synchronized void deleteFiles(String[] paths) throws IOException {
        for (String path : paths) {
            File file = new File(Globals.DATA_DIR + path);
            Files.deleteIfExists(file.toPath());
        }
    }
}
