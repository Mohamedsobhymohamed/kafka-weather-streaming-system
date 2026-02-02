# Kafka Weather Streaming System

This project is a Java-based real-time weather data streaming system built using **Apache Kafka**, **Kafka Streams**, and **PostgreSQL**.  
It simulates weather station data, processes rainfall events in real time, and persists weather readings to a database.

---

## 📌 System Overview

The system consists of four main components:

1. **WeatherStation (Producer)**
   - Simulates a weather station
   - Generates random weather data (temperature, humidity, rainfall, etc.)
   - Publishes JSON messages to a Kafka topic

2. **KafkaConsumerApp (Consumer)**
   - Consumes weather data from Kafka
   - Parses JSON records
   - Stores weather readings into a PostgreSQL database

3. **RainingProcessor (Kafka Streams)**
   - Processes the weather stream in real time
   - Detects raining conditions
   - Sends rainfall alerts to a separate Kafka topic

4. **TestDBConnection**
   - Verifies PostgreSQL database connectivity

---

## 🧱 Architecture

WeatherStation (Producer)
|
v
Kafka Topic: weather_readings
|
+--> KafkaConsumerApp --> PostgreSQL
|
+--> RainingProcessor --> Kafka Topic: rain_alerts


---

## 🛠️ Technologies Used

- Java
- Apache Kafka
- Kafka Streams API
- PostgreSQL
- Maven
- Gson (JSON serialization)

---

## ⚙️ Prerequisites

- Java 8 or higher
- Apache Kafka (running locally)
- PostgreSQL
- Maven

---

## 🗄️ Database Setup

Create a PostgreSQL database:

```sql
CREATE DATABASE weatherdb;
Example table structure:

CREATE TABLE weather_readings (
    id SERIAL PRIMARY KEY,
    station_id INT,
    serial_no BIGINT,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    raining BOOLEAN,
    timestamp TIMESTAMP
);
Update database credentials in the code if needed:

JDBC_URL

DB_USER

DB_PASS

▶️ How to Run
1. Start Kafka
Make sure Zookeeper and Kafka brokers are running locally.

2. Create Kafka Topics
kafka-topics.sh --create --topic weather_readings --bootstrap-server localhost:9092
kafka-topics.sh --create --topic rain_alerts --bootstrap-server localhost:9092
3. Test Database Connection
java com.example.weather.TestDBConnection
4. Start the Kafka Streams Processor
java com.example.weather.RainingProcessor
5. Start the Kafka Consumer
java com.example.weather.KafkaConsumerApp
6. Start the Weather Station Producer
java com.example.weather.WeatherStation
📈 Output
Weather data is continuously produced and consumed

Rain events are detected and forwarded to a dedicated Kafka topic

Weather records are stored in PostgreSQL
