package search.kvs;

import java.util.*;
import java.net.*;
import java.io.*;
import search.tools.HTTP;

/**
 * Implements the KVS interface, providing client-side functionality
 * to interact with a distributed key-value store over a network.
 */
public class KVSClient implements KVS, Serializable {

  // Address of the coordinator in the distributed system.
  String coordinator;

  /**
   * Represents an entry of a worker in the distributed system.
   */
  static class WorkerEntry implements Comparable<WorkerEntry>, Serializable {
    String address; // Network address of the worker.
    String id; // Unique identifier of the worker.

    WorkerEntry(String addressArg, String idArg) {
      address = addressArg;
      id = idArg;
    }

    // Compares this WorkerEntry with another based on the ID.
    public int compareTo(WorkerEntry e) {
      return id.compareTo(e.id);
    }
  };

  Vector<WorkerEntry> workers; // List of workers in the distributed system.
  boolean haveWorkers; // Flag to check if worker list is already fetched.

  // Fetches and returns the number of workers in the system.
  public int numWorkers() throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    return workers.size();
  }

  // Gets the network address of the coordinator of the distributed key-value
  // store.
  public String getCoordinator() {
    return coordinator;
  }

  // Gets the network address of a worker by its index.
  public String getWorkerAddress(int idx) throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    return workers.elementAt(idx).address;
  }

  // Gets the ID of a worker by its index.
  public String getWorkerID(int idx) throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    return workers.elementAt(idx).id;
  }

  // Downloads the list of workers from the coordinator.
  // Parses the response to populate the 'workers' vector.
  synchronized void downloadWorkers() throws IOException {
    String result = new String(HTTP.doRequest("GET", "http://" + coordinator + "/workers", null).body());
    String[] pieces = result.split("\n");
    int numWorkers = Integer.parseInt(pieces[0]);
    if (numWorkers < 1)
      throw new IOException("No active KVS workers");
    if (pieces.length != (numWorkers + 1))
      throw new RuntimeException("Received truncated response when asking KVS coordinator for list of workers");
    workers.clear();
    for (int i = 0; i < numWorkers; i++) {
      String[] pcs = pieces[1 + i].split(",");
      workers.add(new WorkerEntry(pcs[1], pcs[0]));
    }
    Collections.sort(workers);

    haveWorkers = true;
  }

  // Determines the index of the worker that should handle a given key.
  // The key is compared with the IDs of the workers to find the appropriate
  // worker.
  public int workerIndexForKey(String key) {
    int chosenWorker = workers.size() - 1;

    if (key != null) {
      for (int i = 0; i < workers.size() - 1; i++) {
        String id1 = workers.elementAt(i).id;
        String id2 = workers.elementAt(i + 1).id;
        if ((key.compareTo(id1) >= 0) && (key.compareTo(id2) < 0)) {
          chosenWorker = i;
        }
      }
    }

    return chosenWorker;
  }

  // Constructor initializes a new KVSClient with the coordinator's address.
  public KVSClient(String coordinatorArg) {
    coordinator = coordinatorArg;
    workers = new Vector<WorkerEntry>();
    haveWorkers = false;
  }

  // Rename a table
  public boolean rename(String oldTableName, String newTableName) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    boolean result = true;
    for (WorkerEntry w : workers) {
      try {
        byte[] response = HTTP.doRequest("PUT",
            "http://" + w.address + "/rename/" + java.net.URLEncoder.encode(oldTableName, "UTF-8") + "/",
            newTableName.getBytes()).body();
        String res = new String(response);
        result &= res.equals("OK");
      } catch (Exception e) {
      }
    }

    return result;
  }

  // Delete a table
  public void delete(String oldTableName) throws IOException {

    if (!haveWorkers)
      downloadWorkers();
    for (WorkerEntry w : workers) {
      try {
        if (!oldTableName.equals("")) {
          // byte[] response =
          HTTP
              .doRequest("PUT",
                  "http://" + w.address + "/delete/" + java.net.URLEncoder.encode(oldTableName, "UTF-8") + "/", null)
              .body();
          // String result = new String(response);
        }

      } catch (Exception e) {
      }
    }
  }

  // Inserts or updates a value in a specific table, row, and column.
  public void put(String tableName, String row, String column, byte value[]) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    try {
      String target = "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + tableName + "/"
          + java.net.URLEncoder.encode(row, "UTF-8") + "/" + java.net.URLEncoder.encode(column, "UTF-8");

      byte[] response = HTTP.doRequest("PUT", target, value).body();
      String result = new String(response);
      if (!result.equals("OK"))
        throw new RuntimeException("PUT returned something other than OK: " + result + "(" + target + ")");
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException("UTF-8 encoding not supported?!?");
    }
  }

  // Wrapper for put method to accept value as a string.
  public void put(String tableName, String row, String column, String value) throws IOException {
    put(tableName, row, column, value.getBytes());
  }

  // Inserts or updates an entire row in a table.
  public void putRow(String tableName, Row row) throws FileNotFoundException, IOException {
    if (!haveWorkers)
      downloadWorkers();

    System.out.println("http://" + workers.elementAt(workerIndexForKey(row.key())).address + "/data/" + tableName);
    byte[] response = HTTP.doRequest("PUT",
        "http://" + workers.elementAt(workerIndexForKey(row.key())).address + "/data/" + tableName, row.toByteArray())
        .body();
    String result = new String(response);
    if (!result.equals("OK"))
      throw new RuntimeException("PUT returned something other than OK: " + result);
  }

  // Inserts or updates multiple rows in a table.
  public void putRows(String tableName, Map<String, Row> rows) throws IOException {
    Map<Integer, byte[]> maps = new HashMap<>();

    for (String rowKey : rows.keySet()) {
      int workerIndex = workerIndexForKey(rowKey);
      if (!maps.containsKey(workerIndex)) {
        maps.put(workerIndex, rows.get(rowKey).toByteArray());
      } else {
        byte[] a = maps.get(workerIndex);
        byte[] b = rows.get(rowKey).toByteArray();
        byte[] result = new byte[a.length + b.length]; // create a new array with the combined length
        System.arraycopy(a, 0, result, 0, a.length); // copy the first array
        System.arraycopy(b, 0, result, a.length, b.length); // cop
        maps.put(workerIndex, result);
      }
    }

    for (int worker : maps.keySet()) {
      byte[] response = HTTP
          .doRequest("PUT", "http://" + workers.elementAt(worker).address + "/data/" + tableName, maps.get(worker))
          .body();
      String result = new String(response);
      if (!result.equals("OK")) {
        System.out.println(result);
        throw new RuntimeException("PUT returned something other than OK: " + result);

      }
    }
  }

  // Retrieves a row from a specified table.
  public Row getRow(String tableName, String row) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    HTTP.Response resp = HTTP.doRequest("GET", "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/"
        + tableName + "/" + java.net.URLEncoder.encode(row, "UTF-8"), null);
    if (resp.statusCode() == 404)
      return null;

    byte[] result = resp.body();
    try {
      return Row.readFrom(new ByteArrayInputStream(result));
    } catch (Exception e) {
      throw new RuntimeException("Decoding error while reading Row from getRow() URL");
    }
  }

  // Retrieves a specific column value from a table by row and column keys.
  public byte[] get(String tableName, String row, String column) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    HTTP.Response res = HTTP.doRequest("GET",
        "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + tableName + "/"
            + java.net.URLEncoder.encode(row, "UTF-8") + "/" + java.net.URLEncoder.encode(column, "UTF-8"),
        null);
    return ((res != null) && (res.statusCode() == 200)) ? res.body() : null;
  }

  // Checks if a specific row exists in a table.
  public boolean existsRow(String tableName, String row) throws FileNotFoundException, IOException {
    if (!haveWorkers)
      downloadWorkers();

    HTTP.Response r = HTTP.doRequest("GET", "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/"
        + tableName + "/" + java.net.URLEncoder.encode(row, "UTF-8"), null);
    return r.statusCode() == 200;
  }

  // Counts the number of rows in a specified table.
  public int count(String tableName) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    int total = 0;
    for (WorkerEntry w : workers) {
      HTTP.Response r = HTTP.doRequest("GET", "http://" + w.address + "/count/" + tableName, null);
      if ((r != null) && (r.statusCode() == 200)) {
        String result = new String(r.body());
        total += Integer.valueOf(result).intValue();
      }
    }
    return total;
  }

  // Initiates a scan over all rows in the specified table.
  public Iterator<Row> scan(String tableName) throws FileNotFoundException, IOException {
    return scan(tableName, null, null);
  }

  // Initiates a scan over rows in a specified range within the table.
  public Iterator<Row> scan(String tableName, String startRow, String endRowExclusive)
      throws FileNotFoundException, IOException {
    if (!haveWorkers)
      downloadWorkers();

    return new KVSIterator(tableName, startRow, endRowExclusive);
  }

  /**
   * Iterator implementation for iterating over rows in a distributed key-value
   * store.
   */
  class KVSIterator implements Iterator<Row> {
    InputStream in; // Stream for reading rows.
    boolean atEnd; // Flag to indicate if the end of the data stream has been reached.
    Row nextRow; // The next row to be returned by the iterator.
    int currentRangeIndex; // Index to track the current range being processed.
    String endRowExclusive; // Exclusive end row key for scanning.
    String startRow; // Inclusive start row key for scanning.
    String tableName; // Name of the table being scanned.
    Vector<String> ranges; // Vector of URLs to fetch rows from different workers.

    // Constructs a KVSIterator to iterate over rows in a table.
    KVSIterator(String tableNameArg, String startRowArg, String endRowExclusiveArg) throws IOException {
      in = null;
      currentRangeIndex = 0;
      atEnd = false;
      endRowExclusive = endRowExclusiveArg;
      tableName = tableNameArg;
      startRow = startRowArg;
      ranges = new Vector<String>();
      if ((startRowArg == null) || (startRowArg.compareTo(getWorkerID(0)) < 0)) {
        String url = getURL(tableNameArg, numWorkers() - 1, startRowArg,
            ((endRowExclusiveArg != null) && (endRowExclusiveArg.compareTo(getWorkerID(0)) < 0)) ? endRowExclusiveArg
                : getWorkerID(0));
        ranges.add(url);
      }
      for (int i = 0; i < numWorkers(); i++) {
        if ((startRowArg == null) || (i == numWorkers() - 1) || (startRowArg.compareTo(getWorkerID(i + 1)) < 0)) {
          if ((endRowExclusiveArg == null) || (endRowExclusiveArg.compareTo(getWorkerID(i)) > 0)) {
            boolean useActualStartRow = (startRowArg != null) && (startRowArg.compareTo(getWorkerID(i)) > 0);
            boolean useActualEndRow = (endRowExclusiveArg != null)
                && ((i == (numWorkers() - 1)) || (endRowExclusiveArg.compareTo(getWorkerID(i + 1)) < 0));
            String url = getURL(tableNameArg, i, useActualStartRow ? startRowArg : getWorkerID(i),
                useActualEndRow ? endRowExclusiveArg : ((i < numWorkers() - 1) ? getWorkerID(i + 1) : null));
            ranges.add(url);
          }
        }
      }

      openConnectionAndFill();
    }

    // Constructs the URL for a GET request to a worker to scan rows in a table.
    protected String getURL(String tableNameArg, int workerIndexArg, String startRowArg, String endRowExclusiveArg)
        throws IOException {
      String params = "";
      if (startRowArg != null)
        params = "startRow=" + startRowArg;
      if (endRowExclusiveArg != null)
        params = (params.equals("") ? "" : (params + "&")) + "endRowExclusive=" + endRowExclusiveArg;
      return "http://" + getWorkerAddress(workerIndexArg) + "/data/" + tableNameArg
          + (params.equals("") ? "" : "?" + params);
    }

    // Opens a connection to the current range's URL and attempts to fill 'nextRow'.
    void openConnectionAndFill() {
      try {
        if (in != null) {
          in.close();
          in = null;
        }

        if (atEnd)
          return;

        while (true) {
          if (currentRangeIndex >= ranges.size()) {
            atEnd = true;
            return;
          }

          try {
            URL url = new URI(ranges.elementAt(currentRangeIndex)).toURL();
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.connect();
            in = con.getInputStream();
            Row r = fill();
            if (r != null) {
              nextRow = r;
              break;
            }
          } catch (FileNotFoundException fnfe) {
          } catch (URISyntaxException use) {
          }

          currentRangeIndex++;
        }
      } catch (IOException ioe) {
        if (in != null) {
          try {
            in.close();
          } catch (Exception e) {
          }
          in = null;
        }
        atEnd = true;
      }
    }

    // Reads the next row from the input stream.
    synchronized Row fill() {
      try {
        Row r = Row.readFrom(in);
        return r;
      } catch (Exception e) {
        return null;
      }
    }

    // Returns the next row in the iteration.
    public synchronized Row next() {
      if (atEnd)
        return null;
      Row r = nextRow;
      nextRow = fill();
      while ((nextRow == null) && !atEnd) {
        currentRangeIndex++;
        openConnectionAndFill();
      }

      return r;
    }

    // Checks if there are more rows to iterate over.
    public synchronized boolean hasNext() {
      return !atEnd;
    }
  }

  public static void main(String args[]) throws Exception {
    // Check for the minimum number of arguments.
    if (args.length < 2) {
      System.err.println("Syntax: client <coordinator> get <tableName> <row> <column>");
      System.err.println("Syntax: client <coordinator> put <tableName> <row> <column> <value>");
      System.err.println("Syntax: client <coordinator> scan <tableName>");
      System.err.println("Syntax: client <coordinator> delete <tableName>");
      System.err.println("Syntax: client <coordinator> rename <oldTableName> <newTableName>");
      System.exit(1);
    }

    // Create a KVSClient instance with the coordinator's address.
    KVSClient client = new KVSClient(args[0]);

    // Process the command specified in args[1].
    switch (args[1]) {
      case "put":
        if (args.length != 6) {
          System.err.println("Syntax: client <coordinator> put <tableName> <row> <column> <value>");
          System.exit(1);
        }
        client.put(args[2], args[3], args[4], args[5].getBytes("UTF-8"));
        break;

      case "get":
        if (args.length != 5) {
          System.err.println("Syntax: client <coordinator> get <tableName> <row> <column>");
          System.exit(1);
        }
        byte[] val = client.get(args[2], args[3], args[4]);
        if (val == null)
          System.err.println("No value found");
        else
          System.out.write(val);
        break;

      case "scan":
        if (args.length != 3) {
          System.err.println("Syntax: client <coordinator> scan <tableName>");
          System.exit(1);
        }
        Iterator<Row> iter = client.scan(args[2], null, null);
        int count = 0;
        while (iter.hasNext()) {
          System.out.println(iter.next());
          count++;
        }
        System.err.println(count + " row(s) scanned");
        break;

      case "delete":
        if (args.length != 3) {
          System.err.println("Syntax: client <coordinator> delete <tableName>");
          System.exit(1);
        }
        client.delete(args[2]);
        System.err.println("Table '" + args[2] + "' deleted");
        break;

      case "rename":
        if (args.length != 4) {
          System.err.println("Syntax: client <coordinator> rename <oldTableName> <newTableName>");
          System.exit(1);
        }
        if (client.rename(args[2], args[3]))
          System.out.println("Success");
        else
          System.out.println("Failure");
        break;

      default:
        System.err.println("Unknown command: " + args[1]);
        System.exit(1);
        break;
    }
  }
};