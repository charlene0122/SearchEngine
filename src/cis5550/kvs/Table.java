package cis5550.kvs;

import cis5550.tools.KeyEncoder;

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

public class Table {
    public static Map<String, Map<String, Row>> tables = new ConcurrentHashMap<>();

//    public static synchronized void createTable(String tableName, String storageDirectory) {
//        if (tables.get(tableName) == null) {
//            tables.put(tableName, new HashMap<>());
//            if (tableName.startsWith("pt-")) {
//                String fileName = storageDirectory + File.separator + KeyEncoder.encode(tableName);
//                File file = new File(fileName);
//                file.mkdirs();
//            }
//        }
//    }

    // do not put table in memory if it starts with pt
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


    public static synchronized void putRow(String table, String rowKey, Row row, String storageDirectory) {
        if (table.startsWith("pt-")) {
            Path tableDir = Paths.get(storageDirectory, KeyEncoder.encode(table));
            if (!Files.exists(tableDir)) {
                System.out.println("Fail to add Row " + rowKey + " to table " + table + " because it doesn't exist");
                return;
            }
            
            String encodedKey = cis5550.tools.KeyEncoder.encode(rowKey);
            Path rowPath = tableDir.resolve(encodedKey);
            try {
				Files.write(rowPath, row.toByteArray());
			} catch (IOException e) {
				e.printStackTrace();
			}
        } else {
        	tables.computeIfAbsent(table, k -> new ConcurrentHashMap<>())
            .put(rowKey, row);
//        	RowVersion.put(table, rowKey, row);
        }
    }
    
    public static synchronized Row getRow(String tableName, String rowKey, String storageDirectory) {
        if (tableName.startsWith("pt-")) {
            Path rowPath = Paths.get(storageDirectory, tableName, cis5550.tools.KeyEncoder.encode(rowKey));
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
    
    public static ConcurrentMap<String, Row> readRowsFromDirectory(String tableName, String storageDirectory) {
        File tableDirectory = new File(storageDirectory + File.separator + tableName);

        if (!tableDirectory.exists() || !tableDirectory.isDirectory()) {
            return null;
        }

        File[] files = tableDirectory.listFiles();
        if (files == null) {
            return null;
        }

        ConcurrentMap<String, Row> rows = new ConcurrentHashMap<String, Row>();;
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