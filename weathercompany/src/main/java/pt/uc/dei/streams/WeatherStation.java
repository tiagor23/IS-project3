package pt.uc.dei.streams;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.LocalDateTime;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Properties;
import java.util.Random;


public class WeatherStation {
    private Properties props;

    private StreamsBuilder builder;

    public void startCompany(){
        this.setProps();
        assignDbInfo();
        KStream<String, String> standardWeatherStream = builder.stream("standard-weather");
        KStream<String, String> weatherAlertsStream = builder.stream("weather-alerts");
        countTempPerStation(standardWeatherStream);
        countTempPerLocation(standardWeatherStream);
        minMaxPerStation(standardWeatherStream);
        minMaxPerLocation(standardWeatherStream);
        countAlertsPerStation(weatherAlertsStream);
        countAlertsPerType(weatherAlertsStream);
        minTempStationsRedAlerts(standardWeatherStream, weatherAlertsStream);
        maxTempLocationRedLastHour(standardWeatherStream, weatherAlertsStream);
        minTempPerStationRedAlerts(standardWeatherStream, weatherAlertsStream);
        averageTempStation(standardWeatherStream);
        avgTempStationsRedLastHour(standardWeatherStream, weatherAlertsStream);
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();
    }

    private void setProps(){
        builder = new StreamsBuilder();
        props = new Properties();
        String id = Uuid.randomUuid().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, id);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    }

    private void assignDbInfo(){

        KStream<String, String> dbInfoStream = builder.stream("dbInfo_weather_station",
                Consumed.with(Serdes.String(), Serdes.String()));

        // stream to consume the dbInfo
        // produce to standard weather
        dbInfoStream.map((key, result) -> {
            Gson gson = new Gson();
            JsonObject newResult = gson.fromJson(result, JsonObject.class);
            JsonObject value = new JsonObject();
            DecimalFormat decimalFormat = new DecimalFormat("0.0");
            value.add("station", newResult.get("payload").getAsJsonObject().get("name"));
            value.add("location", newResult.get("payload").getAsJsonObject().get("location"));
            value.addProperty("temperature", String.valueOf(decimalFormat.format(Math.random() *
                    (50 - (-10) + 1) + (-10))));
            return new KeyValue<>(newResult.get("payload").getAsJsonObject().get("id").toString(),
                        value.toString());
        }
        ).to("standard-weather");

        // stream to consume the dbInfo
        // produce to weather alerts
        dbInfoStream.map((key, result) -> {
            Gson gson = new Gson();
            JsonObject newResult = gson.fromJson(result, JsonObject.class);
            String[] types = {"green", "red"};
            Random r = new Random();
            JsonObject value = new JsonObject();
            value.add("station", newResult.get("payload").getAsJsonObject().get("name"));
            value.add("location", newResult.get("payload").getAsJsonObject().get("location"));
            value.addProperty("type", types[r.nextInt(types.length)]);
            value.addProperty("date", LocalDateTime.now().toString());
            return new KeyValue<>(newResult.get("payload").getAsJsonObject().get("id").toString(),
                    value.toString());
        }).to("weather-alerts");

    }

    //requirement 1
    private void countTempPerStation(KStream<String, String> standardWeatherStream) {
        standardWeatherStream.map((key, value) -> {
            Gson gson = new Gson();
            JsonObject newResult = gson.fromJson(value, JsonObject.class);
            return new KeyValue<>(newResult.get("station").getAsString(),
                    newResult.get("location").getAsString());
        }).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().toStream().map((k, v) -> {
            JsonObject value = new JsonObject();
            value.addProperty("station", k);
            value.addProperty("count", v);
            return new KeyValue<>(k, value.toString());
        }).to("results-requirement-1");

    }

    //requirement 2
    private void countTempPerLocation(KStream<String, String> standardWeatherStream) {
        standardWeatherStream.map((key, value) -> {
            Gson gson = new Gson();
            JsonObject newResult = gson.fromJson(value, JsonObject.class);
            return new KeyValue<>(newResult.get("location").getAsString(),
                    newResult.get("station").getAsString());
        }).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().toStream().map((k, v) -> {
            JsonObject value = new JsonObject();
            value.addProperty("location", k);
            value.addProperty("count", v);
            return new KeyValue<>(k, value.toString());
        }).to("results-requirement-2");
    }

    //requirement 3
    private void minMaxPerStation(KStream<String, String> standardWeatherStream){
        standardWeatherStream.map((key, value) -> {
            Gson gson = new Gson();
            JsonObject newResult = gson.fromJson(value, JsonObject.class);
            return new KeyValue<>(newResult.get("station").getAsString(),
                    newResult.get("temperature").getAsString());
        }).groupByKey()
                .aggregate(() -> "1000", (k, v, aggregate) -> String.valueOf(Math.min(Double
                        .parseDouble(v.replace(",", ".")),
                        Double.parseDouble(aggregate)))
                ).toStream().map((k, v) -> {
                    JsonObject newResult = new JsonObject();
                    newResult.addProperty("station", k);
                    newResult.addProperty("minTemp", v);
                    return new KeyValue<>(k, newResult.toString());
                }).to("results-requirement-3");
        standardWeatherStream.map((key, value) -> {
                    Gson gson = new Gson();
                    JsonObject newResult = gson.fromJson(value, JsonObject.class);
                    return new KeyValue<>(newResult.get("station").getAsString(),
                            newResult.get("temperature").getAsString());
                }).groupByKey()
                .aggregate(() -> "-1000", (k, v, aggregate) -> String.valueOf(Math.max(Double
                                .parseDouble(v.replace(",", ".")),
                        Double.parseDouble(aggregate)))
                ).toStream().map((k, v) -> {
                    JsonObject newResult = new JsonObject();
                    newResult.addProperty("station", k);
                    newResult.addProperty("maxTemp", v);
                    return new KeyValue<>(k, newResult.toString());
                }).to("results-requirement-3");
    }

    // requirement 4
    private void minMaxPerLocation(KStream<String, String> standardWeatherStream){
        standardWeatherStream.map((key, value) -> {
                    Gson gson = new Gson();
                    JsonObject newResult = gson.fromJson(value, JsonObject.class);
                    return new KeyValue<>(newResult.get("location").getAsString(),
                            newResult.get("temperature").getAsString());
                }).groupByKey()
                .aggregate(() -> "1000", (k, v, aggregate) -> String.valueOf(Math.min(Double
                                .parseDouble(v.replace(",", ".")),
                        Double.parseDouble(aggregate)))
                ).toStream().map((k, v) -> {
                    JsonObject newResult = new JsonObject();
                    newResult.addProperty("location", k);
                    newResult.addProperty("minTemperature", String.valueOf(((Float.parseFloat(v)*9)/5)+32));
                    return new KeyValue<>(k, newResult.toString());
                }).to("results-requirement-4");
        standardWeatherStream.map((key, value) -> {
                    Gson gson = new Gson();
                    JsonObject newResult = gson.fromJson(value, JsonObject.class);
                    return new KeyValue<>(newResult.get("location").getAsString(),
                            newResult.get("temperature").getAsString());
                }).groupByKey()
                .aggregate(() -> "-1000", (k, v, aggregate) -> String.valueOf(Math.max(Double
                                .parseDouble(v.replace(",", ".")),
                        Double.parseDouble(aggregate)))
                ).toStream().map((k, v) -> {
                    JsonObject newResult = new JsonObject();
                    newResult.addProperty("location", k);
                    newResult.addProperty("maxTemperature", String.valueOf(((Float.parseFloat(v)*9)/5)+32));
                    return new KeyValue<>(k, newResult.toString());
                }).to("results-requirement-4");
    }

    //requirement 5
    private void countAlertsPerStation(KStream<String, String> alertsStream) {
        alertsStream.map((key, value) -> {
            Gson gson = new Gson();
            JsonObject newResult = gson.fromJson(value, JsonObject.class);
            return new KeyValue<>(newResult.get("station").getAsString(),
                    newResult.get("location").getAsString());
        }).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().toStream().map((k, v) -> {
            JsonObject value = new JsonObject();
            value.addProperty("station", k);
            value.addProperty("count", v);
            return new KeyValue<>(k, value.toString());
        }).to("results-requirement-5");
    }

    //requirement 6
    private void countAlertsPerType(KStream<String, String> alertsStream) {
        alertsStream.map((key, value) -> {
            Gson gson = new Gson();
            JsonObject newResult = gson.fromJson(value, JsonObject.class);
            return new KeyValue<>(newResult.get("type").getAsString(),
                    newResult.get("location").getAsString());
        }).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().toStream().map((k, v) -> {
            JsonObject value = new JsonObject();
            value.addProperty("type", k);
            value.addProperty("count", v);
            return new KeyValue<>(k, value.toString());
        }).to("results-requirement-6");
    }

    //requirement 7
    private void minTempStationsRedAlerts(KStream<String, String> standardWeatherStream, KStream<String, String> alertsStream){
        ValueJoiner<String, String, String> valueJoiner = (value1, value2) -> {
            Gson gson = new Gson();
            JsonObject newValue1 = gson.fromJson(value1, JsonObject.class);
            JsonObject newValue2 = gson.fromJson(value2, JsonObject.class);
            JsonObject finalValue = new JsonObject();
            finalValue.add("station",
                    newValue2.get("station"));
            finalValue.add("location",
                    newValue2.get("location"));
            finalValue.add("temperature",
                    newValue2.get("temperature"));
            finalValue.add("type",
                    newValue1.get("type"));
            return finalValue.toString();
        };
        alertsStream.filter((k, v) ->
            new Gson().fromJson(v, JsonObject.class).get("type").getAsString().equals("red")
        ).join(standardWeatherStream, valueJoiner,
            JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(0)))
                .map((k, v) -> {
                    Gson gson = new Gson();
                    JsonObject newResult = gson.fromJson(v, JsonObject.class);
                    return new KeyValue<>(newResult.get("station").getAsString(),
                            newResult.get("temperature").getAsString());
                })
                .groupByKey()
                .reduce((aggV, newV) ->
                        Double.parseDouble(newV.replace(",", "."))
                        < Double.parseDouble(aggV.replace(",", "."))
                        ? newV : aggV)
                .toStream().map((k, v) -> {
                    JsonObject newResult = new JsonObject();
                    newResult.addProperty("station", k);
                    newResult.addProperty("minTemperature", v);
                    return new KeyValue<>(k, newResult.toString());
                }).to("results-requirement-7");

    }

    //requirement 8
    private void maxTempLocationRedLastHour(KStream<String, String> standardWeatherStream, KStream<String, String> alerts){
        ValueJoiner<String, String, String> valueJoiner = (value1, value2) -> {
            Gson gson = new Gson();
            JsonObject newValue1 = gson.fromJson(value1, JsonObject.class);
            JsonObject newValue2 = gson.fromJson(value2, JsonObject.class);
            JsonObject finalValue = new JsonObject();
            finalValue.add("station",
                    newValue2.get("station"));
            finalValue.add("location",
                    newValue2.get("location"));
            finalValue.add("temperature",
                    newValue2.get("temperature"));
            finalValue.add("type",
                    newValue1.get("type"));
            return finalValue.toString();
        };
        alerts.filter((k, v) -> {
            JsonObject newValue = new Gson().fromJson(v, JsonObject.class);
            return LocalDateTime.parse(newValue.get("date").getAsString())
                    .isAfter(LocalDateTime.now().minusHours(2));
        }).join(standardWeatherStream, valueJoiner,
            JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(0)))
                .map((k, v) -> {
                    Gson gson = new Gson();
                    JsonObject newResult = gson.fromJson(v, JsonObject.class);
                    return new KeyValue<>(newResult.get("location").getAsString(),
                            newResult.get("temperature").getAsString());
                })
                .groupByKey().reduce((aggV, newV) -> Double.parseDouble(newV.replace(",", "."))
                        > Double.parseDouble(aggV.replace(",", "."))
                        ? newV : aggV).toStream().map((k, v) -> {
                            JsonObject newResult = new JsonObject();
                            newResult.addProperty("location", k);
                            newResult.addProperty("maxTemp", v);
                            return new KeyValue<>(k, newResult.toString());
                }).to("results-requirement-8");
    }

    //requirement 9
    private void minTempPerStationRedAlerts(KStream<String, String> standardWeatherStream, KStream<String,
            String> alertsStream){
        ValueJoiner<String, String, String> valueJoiner = (value1, value2) -> {
            JsonObject finalValue = new JsonObject();
            finalValue.add("type", new Gson().fromJson(value1, JsonObject.class).get("type"));
            finalValue.add("temperature", new Gson().fromJson(value2, JsonObject.class).get("temperature"));
            return finalValue.toString();
        };
        alertsStream.filter((k, v) ->
                        new Gson().fromJson(v, JsonObject.class).get("type").getAsString().equals("red")
                ).join(standardWeatherStream, valueJoiner,
                        JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(0)))
                .map((k, v) -> {
                    Gson gson = new Gson();
                    JsonObject newResult = gson.fromJson(v, JsonObject.class);
                    return new KeyValue<>(newResult.get("type").getAsString(),
                            newResult.get("temperature").getAsString());
                })
                .groupByKey()
                .reduce((aggV, newV) ->
                     Double.parseDouble(newV.replace(",", "."))
                            < Double.parseDouble(aggV.replace(",", "."))
                            ? newV : aggV
                ).toStream().map((k, v) -> {
                    JsonObject newResult = new JsonObject();
                    newResult.addProperty("minTemp", v);
                    return new KeyValue<>(k, newResult.toString());
                }).to("results-requirement-9");
    }

    //requirement 10
    private void averageTempStation(KStream<String, String> standardWeatherStream){
       standardWeatherStream.map((k, v) -> {
           Gson gson = new Gson();
           JsonObject newResult = gson.fromJson(v, JsonObject.class);
           return new KeyValue<>(newResult.get("station").getAsString(), newResult.get("temperature").getAsString());
       }).groupByKey().aggregate(() -> "0,0,0", // first element is the total, second is the average
               (k, v, aggregate) -> {
                    String[] aggregation = aggregate.split(",");
                    int count = Integer.parseInt(aggregation[2]) + 1;
                    double total = Double.parseDouble(aggregation[0]) +
                            Double.parseDouble(v.replace(",", "."));
                    double average = total / count;
                    return total + "," + average + "," + count;
               }
               ).toStream().map((k, v) -> {
                   JsonObject newResult = new JsonObject();
                   newResult.addProperty("station", k);
                   newResult.addProperty("averageTemp", v.split(",")[1]);
                   return new KeyValue<>(k, newResult.toString());
               }).to("results-requirement-10");
    }

    // requirement 11
    private void avgTempStationsRedLastHour(KStream<String, String> standardWeatherStream, KStream<String, String> alerts){
        ValueJoiner<String, String, String> valueJoiner = (value1, value2) -> {
            Gson gson = new Gson();
            JsonObject newValue2 = gson.fromJson(value2, JsonObject.class);
            JsonObject finalValue = new JsonObject();
            finalValue.add("station",
                    newValue2.get("station"));
            finalValue.add("temperature",
                    newValue2.get("temperature"));
            return finalValue.toString();
        };
        alerts.filter((k, v) -> {
                    JsonObject newValue = new Gson().fromJson(v, JsonObject.class);
                    return LocalDateTime.parse(newValue.get("date").getAsString())
                            .isAfter(LocalDateTime.now().minusHours(2))
                            && newValue.get("type").getAsString().equals("red");
                }).join(standardWeatherStream, valueJoiner,
                        JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(0)))
                .map((k, v) -> {
                    Gson gson = new Gson();
                    JsonObject newResult = gson.fromJson(v, JsonObject.class);
                    return new KeyValue<>(newResult.get("station").getAsString(),
                            newResult.get("temperature").getAsString());
                })
                .groupByKey()
                .aggregate(() -> "0,0,0",
                        (k, v, aggregate) -> {
                            String[] aggregation = aggregate.split(",");
                            int count = Integer.parseInt(aggregation[2]) + 1;
                            double total = Double.parseDouble(aggregation[0]) +
                                    Double.parseDouble(v.replace(",", "."));
                            double average = total / count;
                            return total + "," + average + "," + count;
                        }
                ).toStream().map((k, v) -> {
                   JsonObject newResult = new JsonObject();
                   newResult.addProperty("station", k);
                   newResult.addProperty("averageTemp", v.split(",")[1]);
                   return new KeyValue<>(k, newResult.toString());
               }).to("results-requirement-11");
    }

}
