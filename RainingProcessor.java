package com.example.weather;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class RainingProcessor {

    private static final String INPUT_TOPIC = "weather_readings";
    private static final String OUTPUT_TOPIC = "rain_alerts";
    private static final Gson gson = new Gson();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-trigger-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream(INPUT_TOPIC);

        KStream<String, String> rainAlerts = stream
                .filter((key, value) -> {
                    try {
                        JsonObject json = gson.fromJson(value, JsonObject.class);
                        int humidity = json.getAsJsonObject("weather").get("humidity").getAsInt();
                        return humidity > 70;
                    } catch (Exception e) {
                        return false;
                    }
                })
                .mapValues(value -> {
                    JsonObject original = gson.fromJson(value, JsonObject.class);
                    JsonObject alert = new JsonObject();

                    alert.addProperty("station_id", original.get("station_id").getAsLong());
                    alert.addProperty("alert_type", "RAIN");
                    alert.addProperty("message", "Humidity exceeded 70%");
                    alert.addProperty("timestamp", System.currentTimeMillis());

                    System.out.println("🌧 Rain Alert Generated: " + alert);
                    return gson.toJson(alert);
                });

        rainAlerts.to(OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        System.out.println("Raining Trigger Processor started...");

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
