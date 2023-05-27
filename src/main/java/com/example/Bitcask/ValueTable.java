package com.example.Bitcask;

public class ValueTable {

    private int fileId;
    private long offset;
    private long timestamp;

    public ValueTable(int fileId, long offset, long timestamp) {
        this.fileId = fileId;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public int getFileId() {
        return fileId;
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

}
