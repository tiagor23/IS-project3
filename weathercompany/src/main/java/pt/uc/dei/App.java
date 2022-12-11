package pt.uc.dei;

import pt.uc.dei.consumers.WeatherStation;

public class App {
    public static void main(String[] args) {
        WeatherStation weatherStation = new WeatherStation();
        weatherStation.startCompany();

    }
}
