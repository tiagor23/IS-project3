package pt.uc.dei;

import pt.uc.dei.consumers.WeatherCompanyConsumer;

public class App {
    public static void main(String[] args) {
        WeatherCompanyConsumer weatherCompanyConsumer = new WeatherCompanyConsumer();
        weatherCompanyConsumer.startCompany();

    }
}
