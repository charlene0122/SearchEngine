package search.kvs;

import java.util.*;
import java.io.*;

/**
 * The Row class represents a single row in the key-value store. It is capable
 * of storing multiple columns (key-value pairs), where each column value is a
 * byte array.
 * This class also provides functionality for serialization and deserialization
 * of row data for storage and network transmission.
 */
public class Row implements Serializable {

  protected String key;
  protected HashMap<String, byte[]> values;

  // Constructor initializes a new row with the given key.
  public Row(String keyArg) {
    key = keyArg;
    values = new HashMap<String, byte[]>();
  }

  // Returns the key of this row.
  public synchronized String key() {
    return key;
  }

  // Creates and returns a deep copy of this row.
  public synchronized Row clone() {
    Row theClone = new Row(key);
    for (String s : values.keySet())
      theClone.values.put(s, values.get(s));
    return theClone;
  }

  // Returns a set of column keys in this row.
  public synchronized Set<String> columns() {
    return values.keySet();
  }

  // Stores a string value under the specified column key.
  public synchronized void put(String key, String value) {
    values.put(key, value.getBytes());
  }

  // Stores a byte array value under the specified column key.
  public synchronized void put(String key, byte[] value) {
    values.put(key, value);
  }

  // Retrieves a string value for the specified column key.
  public synchronized String get(String key) {
    if (values.get(key) == null)
      return null;
    return new String(values.get(key));
  }

  // Retrieves a byte array value for the specified column key.
  public synchronized byte[] getBytes(String key) {
    return values.get(key);
  }

  // Reads a string from an InputStream until a space is encountered.
  // It's used to parse serialized data where strings are delimited by spaces.
  static String readStringSpace(InputStream in) throws Exception {
    byte buffer[] = new byte[16384];
    int numRead = 0;
    while (true) {
      if (numRead == buffer.length)
        throw new Exception("Format error: Expecting string+space");

      int b = in.read();
      if ((b < 0) || (b == 10))
        return null;
      buffer[numRead++] = (byte) b;
      if (b == ' ')
        return new String(buffer, 0, numRead - 1);
    }
  }

  // Reads a string from a RandomAccessFile until a space is encountered.
  // It's used to parse serialized data where strings are delimited by spaces.
  static String readStringSpace(RandomAccessFile in) throws Exception {
    byte buffer[] = new byte[16384];
    int numRead = 0;
    while (true) {
      if (numRead == buffer.length)
        throw new Exception("Format error: Expecting string+space");

      int b = in.read();
      if ((b < 0) || (b == 10))
        return null;
      buffer[numRead++] = (byte) b;
      if (b == ' ')
        return new String(buffer, 0, numRead - 1);
    }
  }

  // Deserializes a Row object from an InputStream.
  public static Row readFrom(InputStream in) throws Exception {
    String theKey = readStringSpace(in);
    if (theKey == null)
      return null;

    Row newRow = new Row(theKey);
    while (true) {
      String keyOrMarker = readStringSpace(in);
      if (keyOrMarker == null)
        return newRow;

      int len = Integer.parseInt(readStringSpace(in));
      byte[] theValue = new byte[len];
      int bytesRead = 0;
      while (bytesRead < len) {
        int n = in.read(theValue, bytesRead, len - bytesRead);
        if (n < 0)
          throw new Exception("Premature end of stream while reading value for key '" + keyOrMarker + "' (read "
              + bytesRead + " bytes, expecting " + len + ")");
        bytesRead += n;
      }

      byte b = (byte) in.read();
      if (b != ' ')
        throw new Exception("Expecting a space separator after value for key '" + keyOrMarker + "'");

      newRow.put(keyOrMarker, theValue);
    }
  }

  // Deserializes a Row object from a RandomAccessFile.
  public static Row readFrom(RandomAccessFile in) throws Exception {
    String theKey = readStringSpace(in);
    if (theKey == null)
      return null;

    Row newRow = new Row(theKey);
    while (true) {
      String keyOrMarker = readStringSpace(in);
      if (keyOrMarker == null)
        return newRow;

      int len = Integer.parseInt(readStringSpace(in));
      byte[] theValue = new byte[len];
      int bytesRead = 0;
      while (bytesRead < len) {
        int n = in.read(theValue, bytesRead, len - bytesRead);
        if (n < 0)
          throw new Exception("Premature end of stream while reading value for key '" + keyOrMarker + "' (read "
              + bytesRead + " bytes, expecting " + len + ")");
        bytesRead += n;
      }

      byte b = (byte) in.read();
      if (b != ' ')
        throw new Exception("Expecting a space separator after value for key '" + keyOrMarker + "'");

      newRow.put(keyOrMarker, theValue);
    }
  }

  // Converts the row into a string representation.
  public synchronized String toString() {
    String s = key + " {";
    boolean isFirst = true;
    for (String k : values.keySet()) {
      s = s + (isFirst ? " " : ", ") + k + ": " + new String(values.get(k));
      isFirst = false;
    }
    return s + " }";
  }

  // Serializes the row to a byte array for storage or transmission.
  public synchronized byte[] toByteArray() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try {
      baos.write(key.getBytes());
      baos.write(' ');

      for (String s : values.keySet()) {
        baos.write(s.getBytes());
        baos.write(' ');
        baos.write(("" + values.get(s).length).getBytes());
        baos.write(' ');
        baos.write(values.get(s));
        baos.write(' ');
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("This should not happen!");
    }
    ;

    return baos.toByteArray();
  }
}