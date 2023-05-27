package com.example.Bitcask;

public class WeatherStatusSerialized {
    private final long stationId;
    private final ValueSerialized val;

    public WeatherStatusSerialized(long station_id, String battery_status, long status_timestamp,
                                   int humidity, int temperature, int wind_speed) {
        this.stationId = station_id;
        val = new ValueSerialized(battery_status, status_timestamp, humidity,
                temperature, wind_speed);
    }

    public WeatherStatusSerialized(long stationId, ValueSerialized val) {
        this.stationId = stationId;
        this.val = val;
    }

    public Long getKey() {
        return stationId;
    }

    public ValueSerialized getVal() {
        return val;
    }
}
