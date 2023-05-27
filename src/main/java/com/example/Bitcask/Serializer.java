package com.example.Bitcask;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Serializer {

    public static byte[] serialize(WeatherStatusSerialized status) throws IOException {
        Long key = status.getKey();
        ValueSerialized val = status.getVal();
        byte[] b_key = ByteBuffer.allocate(8).putLong(key).array();
        byte[] b_status_timestamp = ByteBuffer.allocate(8).putLong(val.status_timestamp).array();
        byte[] b_bytes = {val.battery_status, val.humidity};
        byte[] b_temperature = ByteBuffer.allocate(4).putInt(val.temperature).array();
        byte[] b_wind_speed = ByteBuffer.allocate(4).putInt(val.wind_speed).array();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(b_key);
        bos.write(b_status_timestamp);
        bos.write(b_bytes);
        bos.write(b_temperature);
        bos.write(b_wind_speed);
        return bos.toByteArray();
    }

    public static WeatherStatusSerialized deserialize(byte[] record) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(record);
        long key = buffer.getLong();
        long status_timestamp = buffer.getLong();
        byte status_battery = buffer.get();
        byte humidity = buffer.get();
        int temperature = buffer.getInt();
        int wind_speed = buffer.getInt();
        return new WeatherStatusSerialized(key, new ValueSerialized(status_battery,
                status_timestamp, humidity, temperature, wind_speed));
    }

}
