package com.example.weather;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestDBConnection {
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/weatherdb";
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "12345";

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASS)) {
            System.out.println("Connected to PostgreSQL!");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
