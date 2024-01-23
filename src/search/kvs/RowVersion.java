package search.kvs;

import java.util.*;

/**
 * The RowVersion class manages versioning for rows in a table. It allows
 * storing multiple versions of a row and retrieving a specific version.
 * Each row is identified uniquely by combining its table name and row key.
 */
public class RowVersion {

    // Map to store all rows with their versions. The key is a combination of table
    // name and row key.
    public static Map<String, TreeMap<Integer, Row>> allRows = new HashMap<>();

    // Stores a new version of a row in the table.
    public static synchronized void put(String table, String rowKey, Row row) {
        allRows.putIfAbsent(table + rowKey, new TreeMap<>());
        TreeMap<Integer, Row> versions = allRows.get(table + rowKey);
        Integer nextVersion = versions.isEmpty() ? 1 : versions.lastKey() + 1;
        versions.put(nextVersion, row.clone());
    }

    // Retrieves a specific version of a row.
    public static synchronized Row get(String table, String rowKey, int version) {
        if (table == null || rowKey == null) {
            return null;
        }
        TreeMap<Integer, Row> versions = allRows.get(table + rowKey);
        if (versions != null) {
            return versions.get(version);
        }
        return null;
    }

    // Gets the current (latest) version number for a row.
    public static synchronized int getCurrentVersion(String table, String rowKey) {
        TreeMap<Integer, Row> versions = allRows.get(table + rowKey);
        return versions != null ? versions.lastKey() : 0;
    }
}
