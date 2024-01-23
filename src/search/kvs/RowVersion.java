package search.kvs;

import java.util.*;

public class RowVersion {
    public static Map<String, TreeMap<Integer, Row>> allRows = new HashMap<>();

    public static synchronized void put(String table, String rowKey, Row row) {
        allRows.putIfAbsent(table + rowKey, new TreeMap<>());
        TreeMap<Integer, Row> versions = allRows.get(table + rowKey);
        Integer nextVersion = versions.isEmpty() ? 1 : versions.lastKey() + 1;
        versions.put(nextVersion, row.clone());
    }

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

    public static synchronized int getCurrentVersion(String table, String rowKey) {
        TreeMap<Integer, Row> versions = allRows.get(table + rowKey);
        return versions != null ? versions.lastKey() : 0;
    }
}
