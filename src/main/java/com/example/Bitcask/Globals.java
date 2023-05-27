package com.example.Bitcask;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Globals {

    public static final int sizeThresh = 680;
    public static final int filesThresh = 5;
    public static final byte step = 34;
    public static final byte record_size = 26;
    public static final String MAIN_DIR = "./bitcask/";
    public static final String DATA_DIR = "./bitcask/data/";

    public static final Map<Long, ValueTable> mainTable = new ConcurrentHashMap<>();
    public static final Map<Long, ValueTable> hintTable = new ConcurrentHashMap<>();

}
