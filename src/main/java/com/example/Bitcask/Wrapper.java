package com.example.Bitcask;

import com.google.gson.Gson;

public class Wrapper {

    private final Gson gson;

    public Wrapper() {
        gson = new Gson();
    }

    public WeatherStatusSerialized convert(String message) {
        WeatherStatusMessage temp = gson.fromJson(message, WeatherStatusMessage.class);
        return new WeatherStatusSerialized(temp.getStation_id(), temp.getBattery_status(),
                temp.getStatus_timestamp(), temp.getHumidity(),
                temp.getTemperature(), temp.getWind_speed());
    }
}
