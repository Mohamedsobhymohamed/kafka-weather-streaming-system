package com.example.weather;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerApp {

    private static final String TOPIC = "weather_readings";
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/weatherdb";
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "12345";

    private static final int BATCH_SIZE = 5000;

    public static void main(String[] args) {

        /* Kafka Consumer Configuration */
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // IMPORTANT

        Gson gson = new Gson();
        int batchCount = 0;

        try (
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASS);
            PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO weather_readings " +
                "(station_id, s_no, battery_status, status_timestamp, humidity, temperature, wind_speed) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)")
        ) {

            conn.setAutoCommit(false); // Required for batching
            consumer.subscribe(Collections.singletonList(TOPIC));

            System.out.println("Central Station Consumer started...");

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {

                    JsonObject json = gson.fromJson(record.value(), JsonObject.class);

                    stmt.setLong(1, json.get("station_id").getAsLong());
                    stmt.setLong(2, json.get("s_no").getAsLong());
                    stmt.setString(3, json.get("battery_status").getAsString());
                    stmt.setLong(4, json.get("status_timestamp").getAsLong());

                    JsonObject weather = json.getAsJsonObject("weather");
                    stmt.setInt(5, weather.get("humidity").getAsInt());
                    stmt.setInt(6, weather.get("temperature").getAsInt());
                    stmt.setInt(7, weather.get("wind_speed").getAsInt());

                    stmt.addBatch();
                    batchCount++;
                }

                /* Execute batch when size reached */
                if (batchCount >= BATCH_SIZE) {
                    stmt.executeBatch();
                    conn.commit();
                    consumer.commitSync();
                    batchCount = 0;
                    System.out.println("Inserted batch of " + BATCH_SIZE + " records");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
