package com.example.Bitcask;

public class WeatherStatusMessage {

    private final long station_id;
    private final long s_no;
    private final String battery_status;
    private final long status_timestamp;
    private final WeatherData weather;

    public WeatherStatusMessage(long station_id, long s_no, String battery_status,
            long status_timestamp, WeatherData weather) {
        this.station_id = station_id;
        this.s_no = s_no;
        this.battery_status = battery_status;
        this.status_timestamp = status_timestamp;
        this.weather = weather;
    }

    private static class WeatherData {
        private final int humidity;
        private final int temperature;
        private final int wind_speed;

        public WeatherData(int humidity, int temperature, int wind_speed) {
            this.humidity = humidity;
            this.temperature = temperature;
            this.wind_speed = wind_speed;
        }
    }

    public long getStation_id() {
        return station_id;
    }

    public String getBattery_status() {
        return battery_status;
    }

    public long getStatus_timestamp() {
        return status_timestamp;
    }

    public int getHumidity() {
        return weather.humidity;
    }

    public int getTemperature() {
        return weather.temperature;
    }

    public int getWind_speed() {
        return weather.wind_speed;
    }
}
