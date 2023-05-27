package com.example.Bitcask;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utility {

    public static int extractId(String path) {
        Pattern pattern = Pattern.compile("file(\\d+)");
        Matcher matcher = pattern.matcher(path);
        if (matcher.find()) {
            String id = matcher.group(1);
            return Integer.parseInt(id);
        } else {
            throw new IllegalArgumentException("Error: Wrong File name Format");
        }
    }

    public static synchronized long write(RandomAccessFile raf, long offset, byte[] record)
            throws IOException {
        long timestamp = System.currentTimeMillis();
        raf.seek(offset);
        raf.writeLong(timestamp);
        raf.write(record);
        return timestamp;
    }

    public static synchronized void replace(File file0, File file1, File temp0, File temp1,
            Map<Long, ValueTable> table, boolean flag) throws IOException, InterruptedException {
        boolean end = false;
        while (!end) {
            try {
                if (!flag) {
                    Files.move(temp0.toPath(), file0.toPath(), StandardCopyOption.ATOMIC_MOVE);
                    Files.move(temp1.toPath(), file1.toPath(), StandardCopyOption.ATOMIC_MOVE);
                }
                Globals.hintTable.clear();
                Globals.hintTable.putAll(table);
                end = true;
            }
            catch (IOException e) {
                if (e instanceof FileSystemException && e.getMessage().
                    contains("being used by another process"))
                    Thread.sleep(150);
                else
                    throw e;
            }
        }
    }
}
