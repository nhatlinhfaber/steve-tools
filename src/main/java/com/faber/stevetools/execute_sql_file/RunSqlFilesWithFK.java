package com.faber.stevetools.execute_sql_file;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.stream.*;

public class RunSqlFilesWithFK {
    public static void main(String[] args) {
        String folderPath = "C:/Users/PC010/Documents/mysql_audience_pro_data_export";  // Path to your folder containing SQL files
        String dbUrl = "jdbc:mysql://localhost:3306/mieruca_audience";  // Database URL
        String dbUser = "root";  // DB Username
        String dbPassword = "faber@2022";  // DB Password

        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            // Disable foreign key checks
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("SET FOREIGN_KEY_CHECKS = 0;");
            }

            // Get all .sql files from the folder
            Files.list(Paths.get(folderPath))
                    .filter(file -> file.toString().endsWith(".sql"))
                    .forEach(file -> executeSqlFile(file, connection));

            // Re-enable foreign key checks after execution
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("SET FOREIGN_KEY_CHECKS = 1;");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void executeSqlFile(Path sqlFile, Connection connection) {
        try {
            // Read the content of the SQL file
            String sql = new String(Files.readAllBytes(sqlFile));
            // Split SQL content by semicolon (;) to handle multiple statements
            String[] sqlStatements = sql.split(";");

            try (Statement stmt = connection.createStatement()) {
                for (String statement : sqlStatements) {
                    statement = statement.trim();
                    if (!statement.isEmpty()) {  // Ignore empty statements
                        stmt.execute(statement);
                        System.out.println("Executed: " + statement);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to execute file: " + sqlFile.getFileName());
            e.printStackTrace();
        }
    }
}
