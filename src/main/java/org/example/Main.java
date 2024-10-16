package org.example;

import org.example.assets.PrimaryKeyItem;
import org.example.assets.TableColsItem;
import org.example.assets.TableListItem;

import org.h2.tools.Server;

import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    static final boolean DEBUG = false;                 // Set to true to see more output in console
    static final boolean USE_IN_MEMORY_DB = true;       // Set to true to use memory DB. If it's false, the app won't initialize the DB, and will use DB_URL
                                                        // to connect to a running H2 DB instead.

    static final String FILE_NAME = "result.log";        // Name of a file where we output the results of the program's work.

    // Change SQL statements below to send different data to H2 tables - only relevant when USE_IN_MEMORY_DB is true.
    static final String TABLE_LIST_INIT_SQL = "INSERT INTO TABLE_LIST(TABLE_NAME, PK) VALUES ('users', 'ID'), ('accounts', 'account, account_id')";
    static final String TABLE_COLS_INIT_SQL = "INSERT INTO TABLE_COLS(TABLE_NAME, COLUMN_NAME, COLUMN_TYPE) VALUES ('users', 'first_name', 'VARCHAR(32)')," +
            "('users', 'second_name', 'VARCHAR(32)'), ('users', 'id', 'INT'), ('accounts', 'register_date', 'TIMESTAMP'), ('accounts', 'CARD_NUMBER', 'INT')," +
            "('accounts', 'ACCOUNT', 'VARCHAR(32)'), ('accounts', 'ACCOUNT_ID', 'INT')";

    // Set DB connectivity data below to your liking.
    static final String DB_URL = "jdbc:h2:mem:test";    // This is a placeholder. Use actual DB URL. Make sure USE_IN_MEMORY_DB is false to use DB_URL.
    static final String USER = "user";
    static final String PASS = "";

    static Server h2Server = null;
    static Connection connection;
    static Statement statement;

    static List<TableColsItem> tableColsItems = new ArrayList<>();
    static List<TableListItem> tableListItems = new ArrayList<>();

    public static void main(String[] args) {

        try {
            Class.forName("org.h2.Driver"); // Load an H2 Driver used by JDBC to establish DB connection

            if (USE_IN_MEMORY_DB) {
                initializeH2Db();
            } else {
                connection = DriverManager.getConnection(DB_URL, USER, PASS);
                statement = connection.createStatement();
                System.out.println("> Connection established to H2 database: " + DB_URL);
            }

            queryH2DbData();

            writePrimaryKeyItemsToFile(getPrimaryKeyItems());

            stopH2Db();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void initializeH2Db() {

        try {

            h2Server = Server.createTcpServer().start();
            if (h2Server.isRunning(true)) {
                System.out.println("> In memory H2 database is up!");
            } else {
                throw new RuntimeException("Could not start in memory H2 database.");
            }

            connection = DriverManager.getConnection("jdbc:h2:mem:test", USER, PASS);
            System.out.println("> Connection established to in-memory H2 database!");
            statement = connection.createStatement();

            statement.executeUpdate("CREATE TABLE TABLE_LIST (TABLE_NAME VARCHAR(32) COMMENT 'имя таблицы', PK VARCHAR(256) COMMENT 'поля первичного ключа, разделитель - запятая')");
            statement.executeUpdate("CREATE TABLE TABLE_COLS (TABLE_NAME VARCHAR(32) COMMENT 'имя таблицы', COLUMN_NAME VARCHAR(32) COMMENT 'имя поля', COLUMN_TYPE VARCHAR(32) COMMENT 'тип данных поля - INT или VARCHAR')");

            statement.executeUpdate(TABLE_LIST_INIT_SQL);
            statement.executeUpdate(TABLE_COLS_INIT_SQL);

            System.out.println("> In memory H2 database is successfully initialized!");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void queryH2DbData() {

        try {
            debugPrintln("DEBUG: data from tables in H2 below:");

            ResultSet results = statement.executeQuery("SELECT * FROM TABLE_LIST");
            while (results.next()) {
                String tableName = results.getString("TABLE_NAME");
                String primaryKeys = results.getString("PK");
                tableListItems.add(new TableListItem(tableName, primaryKeys));

                debugPrintln("    TABLE_LIST (TABLE_NAME, PK) = ('" + tableName + "', '" + primaryKeys + "')");
            }

            results = statement.executeQuery("SELECT * FROM TABLE_COLS");
            while (results.next()) {
                String tableName = results.getString("TABLE_NAME");
                String columnName = results.getString("COLUMN_NAME");
                String columnType = results.getString("COLUMN_TYPE");
                tableColsItems.add(new TableColsItem(tableName, columnName, columnType));

                debugPrintln("    TABLE_COLS (TABLE_NAME, COLUMN_NAME, COLUMN_TYPE) = ('" + tableName + "', '" + columnName + "', '" + columnType + "')");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static List<PrimaryKeyItem> getPrimaryKeyItems() {

        List<PrimaryKeyItem> primaryKeyItems = new ArrayList<>();

        for (TableListItem listItem : tableListItems) {
            Map<String, String> primaryKeysMap = new HashMap<>();

            for (TableColsItem colItem : tableColsItems) {

                if (listItem.getTableName().equals(colItem.getTableName())) {

                    List<String> primaryKeys = listItem.getPrimaryKeys();
                    for (String primaryKey : primaryKeys) {
                        if (primaryKey.equalsIgnoreCase(colItem.getColumnName())) {
                            primaryKeysMap.put(primaryKey, colItem.getColumnType());
                        }
                    }
                }
            }

            if (!primaryKeysMap.isEmpty()) {
                primaryKeyItems.add(new PrimaryKeyItem(listItem.getTableName(), primaryKeysMap));
            }
        }

        if (DEBUG) {
            System.out.println("DEBUG: all PrimaryKeyItems found in data from H2Db:");
            for (PrimaryKeyItem item : primaryKeyItems) {

                Map<String, String> primaryKeyMap = item.getPrimaryKeys();
                for (String key : primaryKeyMap.keySet()) {
                    System.out.println("   TABLE_NAME = '" + item.getTableName() + "', COLUMN_NAME = '" + key + "', COLUMN_TYPE = '" + primaryKeyMap.get(key) + "'");
                }
            }
        }

        return primaryKeyItems;

    }

    private static void writePrimaryKeyItemsToFile(List<PrimaryKeyItem> items) {

        try {
            PrintWriter writer = new PrintWriter(FILE_NAME);

            for (PrimaryKeyItem item : items) {

                String tableName = item.getTableName();
                Map<String, String> primaryKeys = item.getPrimaryKeys();

                for (String key : primaryKeys.keySet()) {
                    writer.println(tableName + ", " + key + ", " + primaryKeys.get(key));
                }
            }
            writer.flush();
            writer.close();

            System.out.println("> Results written to file: " + FILE_NAME);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void stopH2Db() {
        try {
            statement.close();
            connection.close();
            if (h2Server != null) {
                h2Server.stop();
                System.out.println("> In memory H2 database stopped!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void debugPrintln(String message) {
        if (DEBUG) {
            System.out.println(message);
        }
    }
}