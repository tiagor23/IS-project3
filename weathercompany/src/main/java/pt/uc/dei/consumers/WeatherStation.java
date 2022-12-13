package pt.uc.dei.consumers;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
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
        //assignDbInfo();
        KStream<String, String> standardWeatherStream = builder.stream("standard-weather");
        KStream<String, String> weatherAlertsStream = builder.stream("weather-alerts");
        //countTempPerStation(standardWeatherStream);
        //countTempPerLocation(standardWeatherStream);
        //minMaxPerStation(standardWeatherStream);
        //minMaxPerLocation(standardWeatherStream);
        //countAlertsPerStation(weatherAlertsStream);
        //countAlertsPerType(weatherAlertsStream);
        //minTempAllStationsRedAlerts(standardWeatherStream, weatherAlertsStream);
        maxTempLocationRedLastHour(standardWeatherStream, weatherAlertsStream);
        //minTempRedAlerts(standardWeatherStream, weatherAlertsStream);
        //averageTempStation(standardWeatherStream);
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

        KStream<String, String> lines = builder.stream("dbInfo_weather_station",
                Consumed.with(Serdes.String(), Serdes.String()));
        // se me apetecer mudar, meter o id de cada station na location, para identificar a station de cada cena, e meter no topic

        // stream to consume the dbInfo
        // produce to standard weather
        lines.map((key, result) -> {
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
        ).to("standard-weather", Produced.with(Serdes.String(), Serdes.String()));

        // stream to consume the dbInfo
        // produce to weather alerts
        lines.map((key, result) -> {
            Gson gson = new Gson();
            JsonObject newResult = gson.fromJson(result, JsonObject.class);
            String[] types = {"green", "yellow", "red"};
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
    private void countTempPerStation(KStream<String, String> standardWeatherStream) {
        standardWeatherStream.map((key, value) -> {
            Gson gson = new Gson();
            JsonObject newResult = gson.fromJson(value, JsonObject.class);
            return new KeyValue<>(gson.fromJson(newResult.get("station"), JsonElement.class).toString(),
                    gson.fromJson(newResult.get("location"), JsonElement.class).toString());
        }).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().toStream().map((k, v) -> {
            Gson gson = new Gson();
            JsonObject value = new JsonObject();
            value.addProperty("station", k);
            value.addProperty("count", v);
            return new KeyValue<>(k, gson.fromJson(value, JsonObject.class).toString());
        }).to("results-count-temp-station");

    }

    private void countTempPerLocation(KStream<String, String> standardWeatherStream) {
        standardWeatherStream.map((key, value) -> {
            Gson gson = new Gson();
            JsonObject newResult = gson.fromJson(value, JsonObject.class);
            return new KeyValue<>(gson.fromJson(newResult.get("location"), JsonElement.class).toString(),
                    gson.fromJson(newResult.get("station"), JsonElement.class).toString());
        }).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().toStream().map((k, v) -> {
            Gson gson = new Gson();
            JsonObject value = new JsonObject();
            value.addProperty("location", k);
            value.addProperty("count", v);
            return new KeyValue<>(k, gson.fromJson(value, JsonObject.class).toString());
        }).to("results-count-temp-location");
    }

    private void minMaxPerStation(KStream<String, String> standardWeatherStream){
        standardWeatherStream.map((key, value) -> {
            Gson gson = new Gson();
            JsonObject newResult = gson.fromJson(value, JsonObject.class);
            return new KeyValue<>(gson.fromJson(newResult.get("station"), JsonElement.class).toString(),
                    gson.fromJson(newResult.get("temperature"), JsonElement.class).toString());
        }).groupByKey()
                .aggregate(() -> "1000", (k, v, aggregate) -> String.valueOf(Math.min(Double
                        .parseDouble(v.substring(1, v.length() - 1).replace(",", ".")),
                        Double.parseDouble(aggregate)))
                ).toStream().peek((k, v) -> {
                    System.out.println(k);
                    System.out.println(v);
                });
        standardWeatherStream.map((key, value) -> {
                    Gson gson = new Gson();
                    JsonObject newResult = gson.fromJson(value, JsonObject.class);
                    return new KeyValue<>(gson.fromJson(newResult.get("station"), JsonElement.class).toString(),
                            gson.fromJson(newResult.get("temperature"), JsonElement.class).toString());
                }).groupByKey()
                .aggregate(() -> "-1000", (k, v, aggregate) -> String.valueOf(Math.max(Double
                                .parseDouble(v.substring(1, v.length() - 1).replace(",", ".")),
                        Double.parseDouble(aggregate)))
                ).toStream().peek((k, v) -> {
                    System.out.println(k);
                    System.out.println(v);
                });
    }


    private void minMaxPerLocation(KStream<String, String> standardWeatherStream){
        standardWeatherStream.map((key, value) -> {
                    Gson gson = new Gson();
                    JsonObject newResult = gson.fromJson(value, JsonObject.class);
                    return new KeyValue<>(gson.fromJson(newResult.get("location"), JsonElement.class).toString(),
                            gson.fromJson(newResult.get("temperature"), JsonElement.class).toString());
                }).groupByKey()
                .aggregate(() -> "1000", (k, v, aggregate) -> String.valueOf(Math.min(Double
                                .parseDouble(v.substring(1, v.length() - 1).replace(",", ".")),
                        Double.parseDouble(aggregate)))
                ).toStream().peek((k, v) -> {
                    System.out.println(k);
                    System.out.println(v);
                });
        standardWeatherStream.map((key, value) -> {
                    Gson gson = new Gson();
                    JsonObject newResult = gson.fromJson(value, JsonObject.class);
                    return new KeyValue<>(gson.fromJson(newResult.get("location"), JsonElement.class).toString(),
                            gson.fromJson(newResult.get("temperature"), JsonElement.class).toString());
                }).groupByKey()
                .aggregate(() -> "-1000", (k, v, aggregate) -> String.valueOf(Math.max(Double
                                .parseDouble(v.substring(1, v.length() - 1).replace(",", ".")),
                        Double.parseDouble(aggregate)))
                ).toStream().peek((k, v) -> {
                    System.out.println(k);
                    System.out.println(v);
                });
    }
    private void countAlertsPerStation(KStream<String, String> alertsStream) {
        alertsStream.map((key, value) -> {
            Gson gson = new Gson();
            JsonObject newResult = gson.fromJson(value, JsonObject.class);
            return new KeyValue<>(gson.fromJson(newResult.get("station"), JsonElement.class).toString(),
                    gson.fromJson(newResult.get("location"), JsonElement.class).toString());
        }).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().toStream().map((k, v) -> {
            Gson gson = new Gson();
            JsonObject value = new JsonObject();
            value.addProperty("station", k);
            value.addProperty("count", v);
            return new KeyValue<>(k, gson.fromJson(value, JsonObject.class).toString());
        }).peek((k, v) -> System.out.println(k + "->" + v));
    }
    private void countAlertsPerType(KStream<String, String> alertsStream) {
        alertsStream.map((key, value) -> {
            Gson gson = new Gson();
            JsonObject newResult = gson.fromJson(value, JsonObject.class);
            return new KeyValue<>(gson.fromJson(newResult.get("type"), JsonElement.class).toString(),
                    gson.fromJson(newResult.get("location"), JsonElement.class).toString());
        }).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().toStream().map((k, v) -> {
            Gson gson = new Gson();
            JsonObject value = new JsonObject();
            value.addProperty("type", k);
            value.addProperty("count", v);
            return new KeyValue<>(k, gson.fromJson(value, JsonObject.class).toString());
        }).peek((k, v) -> System.out.println(k + "->" + v));
    }

    //requirement 7
    private void minTempAllStationsRedAlerts(KStream<String, String> standardWeatherStream, KStream<String, String> alertsStream){
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
                    return new KeyValue<>(newResult.get("station").toString(),
                            newResult.get("temperature").toString());
                })
                .groupByKey()
                .reduce((s, v1) -> Double.parseDouble(v1.substring(1, v1.length() - 1).replace(",", "."))
                        < Double.parseDouble(s.substring(1, s.length() - 1).replace(",", "."))
                        ? v1 : s).toStream().peek((k, v) -> {
                    System.out.println(k);
                    System.out.println(v);
                });
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
            return !LocalDateTime.parse(newValue.get("date").getAsString())
                    .isBefore(LocalDateTime.parse(newValue.get("date").getAsString()).minusHours(1));
        }).join(standardWeatherStream, valueJoiner,
            JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(0)))
                .map((k, v) -> {
                    Gson gson = new Gson();
                    JsonObject newResult = gson.fromJson(v, JsonObject.class);
                    return new KeyValue<>(newResult.get("location").toString(),
                            newResult.get("temperature").toString());
                })
                .groupByKey().reduce((s, v1) -> Double.parseDouble(v1.substring(1, v1.length() - 1)
                        .replace(",", "."))
                        > Double.parseDouble(s.substring(1, s.length() - 1).replace(",", "."))
                        ? v1 : s).toStream().peek((k, v) -> {
                    System.out.println(k);
                    System.out.println(v);
                });
    }

    //requirement 9
    private void minTempPerStationRedAlerts(KStream<String, String> standardWeatherStream, KStream<String, String> alertsStream){
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
                    return new KeyValue<>(newResult.get("station").toString(),
                            newResult.get("temperature").toString());
                })
                .groupByKey()
                .reduce((s, v1) -> Double.parseDouble(v1.substring(1, v1.length() - 1).replace(",", "."))
                        < Double.parseDouble(s.substring(1, s.length() - 1).replace(",", "."))
                        ? v1 : s).toStream().peek((k, v) -> {
                    System.out.println(k);
                    System.out.println(v);
                });
    }

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
               ).toStream().peek((k, v) -> {System.out.println(k);
           System.out.println(v.split(",")[1]);});
    }

    private void avgTempStationsRedLastHour(KStream<String, String> standardWeatherStream, KStream<String, String> alerts){
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
                    return !LocalDateTime.parse(newValue.get("date").getAsString())
                            .isBefore(LocalDateTime.parse(newValue.get("date").getAsString()).minusHours(1))
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
                .aggregate(() -> "0,0,0", // first element is the total, second is the average
                        (k, v, aggregate) -> {
                            String[] aggregation = aggregate.split(",");
                            int count = Integer.parseInt(aggregation[2]) + 1;
                            double total = Double.parseDouble(aggregation[0]) +
                                    Double.parseDouble(v.replace(",", "."));
                            double average = total / count;
                            return total + "," + average + "," + count;
                        }
                ).toStream().peek((k, v) -> {
                    System.out.println("-------------------------------------------");
                    System.out.println(k);
                    System.out.println(v.split(",")[1]);});
    }

}
