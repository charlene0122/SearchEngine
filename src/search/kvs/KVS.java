package search.kvs;

import java.util.Iterator;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * The KVS interface defines the operations for a key-value store.
 * It includes methods for manipulating and querying data in tables.
 */
public interface KVS {
  // Inserts or updates a value in a table.
  void put(String tableName, String row, String column, byte[] value) throws FileNotFoundException, IOException;

  // Inserts or updates an entire row in a table.
  void putRow(String tableName, Row row) throws FileNotFoundException, IOException;

  // Retrieves a row from a table by row key.
  Row getRow(String tableName, String row) throws FileNotFoundException, IOException;

  // Checks if a specific row exists in a table.
  boolean existsRow(String tableName, String row) throws FileNotFoundException, IOException;

  // Retrieves a specific value from a table by row and column keys.
  byte[] get(String tableName, String row, String column) throws FileNotFoundException, IOException;

  // Provides an iterator for scanning rows within a specified key range.
  Iterator<Row> scan(String tableName, String startRow, String endRowExclusive)
      throws FileNotFoundException, IOException;

  // Counts the number of rows in a table.
  int count(String tableName) throws FileNotFoundException, IOException;

  // Renames a table to a new name.
  boolean rename(String oldTableName, String newTableName) throws IOException;

  // Deletes a table from the store.
  void delete(String oldTableName) throws IOException;
};