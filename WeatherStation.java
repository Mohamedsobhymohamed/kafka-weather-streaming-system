package com.example.weather;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class WeatherStation {

    private static final int STATION_ID = 1;
    private static long serialNo = 0;

    private static final Random random = new Random();
    private static final Gson gson = new Gson();

    public static void main(String[] args) throws InterruptedException {

        // Kafka configuration
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        while (true) {
            serialNo++;

            // 🔴 Drop 10% of messages
            if (random.nextInt(100) < 10) {
                Thread.sleep(1000);
                continue;
            }

            String batteryStatus = generateBatteryStatus();

            long timestamp = System.currentTimeMillis() / 1000;

            int humidity = random.nextInt(101);
            int temperature = 30 + random.nextInt(91);
            int windSpeed = random.nextInt(61);

            // Weather object
            Map<String, Object> weather = new HashMap<>();
            weather.put("humidity", humidity);
            weather.put("temperature", temperature);
            weather.put("wind_speed", windSpeed);

            // Full message
            Map<String, Object> message = new HashMap<>();
            message.put("station_id", STATION_ID);
            message.put("s_no", serialNo);
            message.put("battery_status", batteryStatus);
            message.put("status_timestamp", timestamp);
            message.put("weather", weather);

            String jsonMessage = gson.toJson(message);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("weather_readings", String.valueOf(STATION_ID), jsonMessage);

            producer.send(record);

            System.out.println(jsonMessage); // for debugging (can remove later)

            Thread.sleep(1000);
        }
    }

    // ✅ Correct 30% / 40% / 30% battery distribution
    private static String generateBatteryStatus() {
        int r = random.nextInt(100);
        if (r < 30) return "low";
        else if (r < 70) return "medium";
        else return "high";
    }
}
