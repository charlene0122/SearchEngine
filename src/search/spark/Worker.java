package search.Spark;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.net.*;
import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static search.kvs.Worker.generateId;
import static search.webserver.Server.*;
import search.tools.Hasher;
import search.tools.Serializer;
import search.Spark.SparkContext.RowToString;
import search.kvs.*;
import search.webserver.Request;

class Worker extends search.generic.Worker {

    public static void main(String args[]) {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        System.out.println("worker initiated on port " + port);
        String server = args[1];

        // for Spark worker workerID is not assigned in the beginning, using !! to send
        // to coordinator and use IP instead
        startPingThread(server, "!!", port);
        final File myJAR = new File("__worker" + port + "-current.jar");

        port(port);

        post("/useJAR", (request, response) -> {
            FileOutputStream fos = new FileOutputStream(myJAR);
            fos.write(request.bodyAsBytes());
            fos.close();
            return "OK";
        });

        post("/rdd", (request, response) -> {
            String inputTable = request.queryParams("input");
            String outputTable = request.queryParams("output");
            String jarName = request.queryParams("jar");
            String kvsCoordinator = request.queryParams("kvsCoordinator");
            String fromKey = request.queryParams("from");
            String toKeyExclusive = request.queryParams("to");
            String operation = request.queryParams("oper");

            if (inputTable == null || outputTable == null || kvsCoordinator == null || operation == null) {
                response.status(400, "Bad request");
                return "400 Bad Request- missing parameters";
            }
            Coordinator.kvs = new KVSClient(kvsCoordinator);

            if (fromKey.equals("!!")) {
                fromKey = null;
            }
            if (toKeyExclusive.equals("!!")) {
                toKeyExclusive = null;
            }

            if (!myJAR.exists()) {
                System.out.println("404 Not found- jar file not found");
                response.status(404, "Not found");
                return "404 Not found- jar file not found";
            }

            Iterator<Row> rows = Coordinator.kvs.scan(inputTable, fromKey, toKeyExclusive);

            int index = 1;
            String prefix = inputTable + System.currentTimeMillis();
            switch (operation) {
                case "flatMap":
                    SparkRDD.StringToIterable flatMap = (SparkRDD.StringToIterable) Serializer
                            .byteArrayToObject(request.bodyAsBytes(), myJAR);
                    index = 1;

                    while (rows.hasNext()) {
                        Row row = rows.next();
                        Iterable<String> iter = flatMap.op(row.get("value"));
                        if (iter != null) {
                            for (String val : iter) {
                                String rowKey = Hasher.hash(prefix + index++);
                                Coordinator.kvs.put(outputTable, rowKey, "value", val);
                            }
                        }
                    }
                    break;

                case "mapToPair":
                    SparkRDD.StringToPair mapToPair = (SparkRDD.StringToPair) Serializer
                            .byteArrayToObject(request.bodyAsBytes(), myJAR);
                    index = 1;
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        SparkPair pair = mapToPair.op(row.get("value"));
                        // String rowKey = Hasher.hash(prefix + index++);
                        Coordinator.kvs.put(outputTable, pair._1(), row.key(), pair._2());
                    }
                    break;
                // SparkRDD.StringToPair mapToPair = (SparkRDD.StringToPair)
                // Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
                // index = 1;
                // HashMap<String, Row> cache = new HashMap<>();
                // while (rows.hasNext()) {
                // Row row = rows.next();
                // SparkPair pair = mapToPair.op(row.get("value"));
                // String rowKey = Hasher.hash(prefix + index++);
                // Row newRow = new Row(pair._1());
                // newRow.put(rowKey, pair._2().getBytes(StandardCharsets.UTF_8));
                // cache.put(rowKey, newRow);
                // if (cache.size() > 1000) {
                // Coordinator.kvs.putRows(outputTable, cache);
                // cache = new HashMap<>();
                // }
                // }
                // if (cache.size() > 0) {
                // Coordinator.kvs.putRows(outputTable, cache);
                // }
                // break;

                case "foldByKey":
                    String zeroElement = request.queryParams("zero");
                    if (zeroElement == null) {
                        zeroElement = "";
                    }
                    if (zeroElement == null) {
                        response.status(400, "Bad request");
                        return "400 Bad Request- missing zeroElement param for foldByKey";
                    }
                    SparkPairRDD.TwoStringsToString foldByKey = (SparkPairRDD.TwoStringsToString) Serializer
                            .byteArrayToObject(request.bodyAsBytes(), myJAR);
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        String value = zeroElement;
                        index = 1;
                        for (String column : row.columns()) {
                            value = foldByKey.op(value, row.get(column));
                            index++;
                        }
                        Coordinator.kvs.put(outputTable, row.key(), "acc", value.getBytes());
                    }
                    return index;

                case "fold":

                    SparkPairRDD.TwoStringsToString fold = (SparkPairRDD.TwoStringsToString) Serializer
                            .byteArrayToObject(request.bodyAsBytes(), myJAR);
                    String zero = request.queryParams("zero2");
                    if (zero == null) {
                        response.status(400, "Bad request");
                        return "400 Bad Request - missing zeroElement param for fold";
                    }
                    String accumulatedValue = zero;

                    rows = Coordinator.kvs.scan(inputTable);
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        for (String column : row.columns()) {
                            String columnValue = row.get(column);
                            accumulatedValue = fold.op(accumulatedValue, row.get(column));
                        }
                    }
                    Coordinator.kvs.put(outputTable, prefix, "value", accumulatedValue);
                    break;

                case "intersection":
                    String other = request.queryParams("that");
                    if (other == null) {
                        response.status(400, "Bad request");
                        return "400 Bad Request- missing parameter";
                    }
                    Set<String> thisValues = new HashSet<>();
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        thisValues.add(row.get("value"));
                    }

                    Iterator<Row> otherRows = Coordinator.kvs.scan(other);
                    Set<String> otherValues = new HashSet<>();
                    while (otherRows.hasNext()) {
                        Row row = otherRows.next();
                        otherValues.add(row.get("value"));
                    }

                    thisValues.retainAll(otherValues);
                    index = 1;
                    for (String value : thisValues) {
                        String rowKey = Hasher.hash(prefix + index++);
                        Coordinator.kvs.put(outputTable, rowKey, "value", value);
                    }
                    break;

                case "sample":
                    String possibility = request.queryParams("prob");
                    if (possibility == null) {
                        response.status(400, "Bad request");
                        return "400 Bad Request- missing possiblility param for sample";
                    }
                    index = 1;
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        if (Math.random() < Double.parseDouble(possibility)) {
                            String rowKey = Hasher.hash(prefix + index++);
                            Coordinator.kvs.put(outputTable, rowKey, "value", row.get("value"));
                        }
                    }
                    break;

                case "flatMapToPair":
                    SparkRDD.StringToPairIterable flatMapToPair = (SparkRDD.StringToPairIterable) Serializer
                            .byteArrayToObject(request.bodyAsBytes(), myJAR);
                    index = 1;
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        Iterable<SparkPair> pairs = flatMapToPair.op(row.get("value"));
                        for (SparkPair pair : pairs) {
                            String hash = row.key() + "-" + index++;
                            Coordinator.kvs.put(outputTable, pair._1(), hash, pair._2());
                        }
                    }
                    break;
                case "flatMapFromPair":
                    SparkPairRDD.PairToStringIterable flatMapFromPair = (SparkPairRDD.PairToStringIterable) Serializer
                            .byteArrayToObject(request.bodyAsBytes(), myJAR);
                    index = 1;
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        for (String column : row.columns()) {
                            SparkPair pair = new SparkPair(row.key(), row.get(column));
                            Iterable<String> iter = flatMapFromPair.op(pair);
                            if (iter != null) {
                                for (String result : iter) {
                                    if (result != null) {
                                        String rowKey = Hasher.hash(prefix + index++);
                                        Coordinator.kvs.put(outputTable, rowKey, "value", result);
                                    }
                                }
                            }
                        }
                    }
                    break;
                case "flatMapToPairFromPair":
                    SparkPairRDD.PairToPairIterable flatMapToPair2 = (SparkPairRDD.PairToPairIterable) Serializer
                            .byteArrayToObject(request.bodyAsBytes(), myJAR);
                    index = 1;
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        for (String column : row.columns()) {
                            SparkPair pair = new SparkPair(row.key(), row.get(column));
                            Iterable<SparkPair> pairs = flatMapToPair2.op(pair);
                            for (SparkPair p : pairs) {
                                String hash = row.key() + "-" + index++;
                                Coordinator.kvs.put(outputTable, p._1(), hash, p._2());
                            }
                        }
                    }
                    break;

                case "join":
                    String otherTable = request.queryParams("otherTable");
                    if (otherTable == null) {
                        System.out.println("400!!!");
                        response.status(400, "Bad request");
                        return "400 Bad Request - missing other table parameter for join";
                    }
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        Row otherRow = Coordinator.kvs.getRow(otherTable, row.key());
                        if (otherRow != null) {
                            for (String col : row.columns()) {
                                for (String otherCol : otherRow.columns()) {
                                    String joinedValue = row.get(col) + "," + otherRow.get(otherCol);
                                    String hash = prefix + joinedValue;
                                    Coordinator.kvs.put(outputTable, row.key(), hash, joinedValue);
                                }
                            }
                        }
                    }
                    break;

            }

            return "OK";
        });

        post("/fromTable", (request, response) -> {
            String inputTable = request.queryParams("input");
            String outputTable = request.queryParams("output");
            String fromKey = request.queryParams("from");
            String toKeyExclusive = request.queryParams("to");
            String kvsCoordinator = request.queryParams("kvsCoordinator");
            String jarName = request.queryParams("jar");

            if (inputTable == null || outputTable == null) {
                response.status(400, "Bad request");
                return "400 Bad Request- missing parameters";
            }
            KVSClient kvs = new KVSClient(kvsCoordinator);

            if (!myJAR.exists()) {
                response.status(404, "Not found");
                return "404 Not found- jar file not found";
            }

            Iterator<Row> rows = kvs.scan(inputTable, fromKey.equals("!!") ? null : fromKey,
                    toKeyExclusive.equals("!!") ? null : toKeyExclusive);
            RowToString lambda = (RowToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

            int index = 1;
            String res200 = "200";
            while (rows.hasNext()) {
                Row row = rows.next();
                if (row.get("url").contains("..")) {
                    continue;
                }
                if (row.get("url").length() > 100) {
                    continue;
                }
                if (res200.equals(row.get("responseCode")) && row.get("contentType") != null
                        && row.get("contentType").contains("text/html")
                        && row.get("contentType").toLowerCase().contains("utf-8")) {
                    String result = lambda.op(row);
                    if (result != null) {
                        String rowKey = Hasher.hash(inputTable + System.currentTimeMillis() + index++);
                        // String rowKey = Hasher.hash(inputTable + index++);
                        kvs.put(outputTable, rowKey, "value", result);
                    }
                }
            }

            return "OK";
        });

    }

}
