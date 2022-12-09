package pt.uc.dei.consumers;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import java.util.Properties;
import java.util.Random;


public class WeatherCompanyConsumer {
    private Properties props;


    public void startCompany(){
        this.setProps();
        assignDbInfo();
    }

    private void setProps(){
        props = new Properties();
        String id = Uuid.randomUuid().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, id);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    }

    private void assignDbInfo(){

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines = builder.stream("dbInfo-weather_stations",
                Consumed.with(Serdes.String(), Serdes.String()));

        // stream to consume the dbInfo
        // produce to standard weather
        lines.map((key, result) -> {
            Gson gson = new Gson();
            JsonObject newResult = gson.fromJson(result, JsonObject.class);
            JsonObject value = new JsonObject();
            value.addProperty("location", newResult.get("payload").getAsJsonObject().get("location").getAsString());
            value.addProperty("temperature", String.valueOf(Math.random() * (50 - (-10) + 1) + (-10)));
            return new KeyValue<>(newResult.get("payload").getAsJsonObject().get("id").getAsString(),
                        gson.fromJson(value, JsonObject.class).toString());
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
            value.addProperty("location", newResult.get("payload").getAsJsonObject().get("location").getAsString());
            value.addProperty("type", types[r.nextInt(types.length)]);
            return new KeyValue<>(newResult.get("payload").getAsJsonObject().get("id").getAsString(),
                    gson.fromJson(value, JsonObject.class).toString());
        }).to("weather-alerts");
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();
    }



}
