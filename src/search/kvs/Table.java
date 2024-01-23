package search.kvs;

import search.tools.KeyEncoder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * The Table class represents the data storage structure for a key-value store
 * system.
 * It provides methods for creating tables, adding and retrieving rows, and
 * counting rows.
 * Tables can be stored either in-memory or persistently on disk.
 */
public class Table {
    public static Map<String, Map<String, Row>> tables = new ConcurrentHashMap<>();

    /**
     * Creates a new table either in-memory or on disk based on the table name.
     * Tables prefixed with "pt-" are persisted on disk.
     * 
     * @param tableName        The name of the table to create.
     * @param storageDirectory Directory for storing persistent tables.
     */
    public static synchronized void createTable(String tableName, String storageDirectory) {
        if (tableName.startsWith(("pt-"))) {
            if (!Files.exists(Paths.get(storageDirectory, KeyEncoder.encode(tableName)))) {
                String fileName = storageDirectory + File.separator + KeyEncoder.encode(tableName);
                File file = new File(fileName);
                file.mkdirs();
            }
        } else {
            if (tables.get(tableName) == null) {
                tables.put(tableName, new HashMap<>());
            }
        }
    }

    // Adds or updates a row in the specified table.
    public static synchronized void putRow(String table, String rowKey, Row row, String storageDirectory) {
        if (table.startsWith("pt-")) {
            Path tableDir = Paths.get(storageDirectory, KeyEncoder.encode(table));
            if (!Files.exists(tableDir)) {
                System.out.println("Fail to add Row " + rowKey + " to table " + table + " because it doesn't exist");
                return;
            }

            String encodedKey = search.tools.KeyEncoder.encode(rowKey);
            Path rowPath = tableDir.resolve(encodedKey);
            try {
                Files.write(rowPath, row.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            tables.computeIfAbsent(table, k -> new ConcurrentHashMap<>())
                    .put(rowKey, row);
            // RowVersion.put(table, rowKey, row);
        }
    }

    // Retrieves a row from a specified table.
    public static synchronized Row getRow(String tableName, String rowKey, String storageDirectory) {
        if (tableName.startsWith("pt-")) {
            Path rowPath = Paths.get(storageDirectory, tableName, search.tools.KeyEncoder.encode(rowKey));
            if (Files.exists(rowPath)) {
                try (InputStream in = Files.newInputStream(rowPath)) {
                    return Row.readFrom(in);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                return null;
            }
        }
        if (tableName == null || rowKey == null || !tables.containsKey(tableName)
                || !tables.get(tableName).containsKey(rowKey)) {
            return null;
        }
        return tables.get(tableName).get(rowKey);

    }

    // Reads rows from a directory into a concurrent map. Used for persistent
    // tables.
    public static ConcurrentMap<String, Row> readRowsFromDirectory(String tableName, String storageDirectory) {
        File tableDirectory = new File(storageDirectory + File.separator + tableName);

        if (!tableDirectory.exists() || !tableDirectory.isDirectory()) {
            return null;
        }

        File[] files = tableDirectory.listFiles();
        if (files == null) {
            return null;
        }

        ConcurrentMap<String, Row> rows = new ConcurrentHashMap<String, Row>();
        ;
        for (File file : files) {
            try (FileInputStream fis = new FileInputStream(file)) {
                Row row = Row.readFrom(fis);
                if (row != null) {
                    rows.put(row.key, row);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return rows;
    }

    // Counts the number of rows in a table.
    public static int countRows(String tableName, String storageDirectory) {
        File tableDirectory = new File(storageDirectory + File.separator + tableName);

        if (!tableDirectory.exists() || !tableDirectory.isDirectory()) {
            return -1;
        }

        File[] files = tableDirectory.listFiles();
        if (files == null) {
            return 0;
        }

        return files.length;
    }
}