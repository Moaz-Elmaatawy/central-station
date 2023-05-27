package com.example.Bitcask;

public class ValueSerialized {
    public final long status_timestamp;
    // Don't have to be stored as String.
    public final byte battery_status;
    // Don't need whole 4 bytes as it's a percentage.
    public final byte humidity;
    public final int temperature;
    public final int wind_speed;

    public ValueSerialized(String battery_status, long status_timestamp, int humidity,
                           int temperature, int wind_speed) {
        this.status_timestamp = status_timestamp;
        this.battery_status = batteryConverter(battery_status);
        this.humidity = (byte) humidity;
        this.temperature = temperature;
        this.wind_speed = wind_speed;
    }

    public ValueSerialized(byte battery_status, long status_timestamp, byte humidity,
                           int temperature, int wind_speed) {
        this.status_timestamp = status_timestamp;
        this.battery_status = battery_status;
        this.humidity = humidity;
        this.temperature = temperature;
        this.wind_speed = wind_speed;
    }

    public byte batteryConverter(String battery_status) {
        if (battery_status.equalsIgnoreCase("LOW"))
            return 1;
        else if (battery_status.equalsIgnoreCase("MEDIUM"))
            return 2;
        else
            return 3;
    }
}
