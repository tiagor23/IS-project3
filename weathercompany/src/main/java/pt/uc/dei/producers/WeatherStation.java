package pt.uc.dei.producers;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;


public class WeatherStation {
    
        /*
    public void produceTopics() {
        // create instance for properties to access producer configs
        Properties props = new Properties();
        Serde serde = Serdes.serdeFrom(new JsonPOJOSerializer<>(), new JsonPOJODeserializer<>());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-station");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");


    }

    private static void countTemperaturePerStation(Properties props){
        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, StandardWeather> source = builder
                .stream("standard-weather", Consumed.with(Serdes.String(),
                        Serdes.serdeFrom(new JsonPOJOSerializer<>(), new JsonPOJODeserializer<>())));

        /*final KTable<String, Results> counts = source
                .groupBy((key, value) -> value.getGenre())
                .count()
                .mapValues(GenreCount::new);

// need to override value serde to GenreCount type
        counts
                .toStream()
                .to("results", Produced.with(Serdes.String(),
                        Serdes.serdeFrom(new JsonPOJOSerializer<>(), new JsonPOJODeserializer<>())));
        System.out.println("Message sent successfully");
    }
        */
}

