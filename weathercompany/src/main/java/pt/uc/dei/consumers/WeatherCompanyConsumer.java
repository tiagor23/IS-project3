package pt.uc.dei.consumers;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

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
        lines.map((key, result) -> {
                    Gson gson = new Gson();
                    JsonObject newResult = gson.fromJson(result, JsonObject.class);
                    return new KeyValue<>(newResult.get("payload").getAsJsonObject().get("id").getAsString(),
                            newResult.get("payload").getAsJsonObject().get("name").getAsString());
                }
        ).to("test", Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();



    }


}
