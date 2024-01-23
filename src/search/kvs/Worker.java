package search.kvs;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import search.tools.KeyEncoder;
import search.tools.Logger;
import search.webserver.Server;

public class Worker extends search.generic.Worker {
	public static Logger logger = Logger.getLogger(Worker.class);

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Please provide the required arguments: <port> <storage directory> <ip:port>");
			System.exit(1);
		}

		int port = Integer.parseInt(args[0]);
		if (port <= 0 || port > 65535) {
			System.out.println("Please provide a valid port number between 1 and 65535.");
			System.exit(1);
		}

		String storageDirectory = args[1];
		String ipPort = args[2];
		String workerId = null;
		try {
			if (!(new File(storageDirectory)).exists()) {
				(new File(storageDirectory)).mkdir();
			}

			File file = new File(storageDirectory + File.separator + "id");
			if (file.exists()) {
				workerId = (new Scanner(file)).nextLine();
			}

			if (workerId == null) {
				workerId = generateId();
				BufferedWriter writer = new BufferedWriter(new FileWriter(file));
				writer.write(workerId);
				writer.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		Server.port(port);
		startPingThread(ipPort, workerId, port);

		Server.get("/", (req, res) -> {
			StringBuilder html = new StringBuilder("<html><body>");
			html.append("<table border='1'><tr><th>Table Name</th><th>Number of Keys</th></tr>");

			for (String tableName : Table.tables.keySet()) {
				html.append("<tr><td><a href='/view/").append(tableName).append("'>").append(tableName)
						.append("</a></td>")
						.append("<td>").append(Table.tables.get(tableName).size()).append("</td></tr>");
			}

			try (Stream<Path> paths = Files.walk(Paths.get(storageDirectory), 1)) {
				paths
						.filter(Files::isDirectory)
						.filter(path -> !path.equals(Paths.get(storageDirectory)))
						.map(path -> path.getFileName().toString())
						.forEach(tableName -> {
							try (Stream<Path> filePaths = Files.list(Paths.get(storageDirectory).resolve(tableName))) {
								long numberOfKeys = filePaths.filter(Files::isRegularFile).count();
								html.append("<tr><td><a href='/view/").append(tableName).append("'>").append(tableName)
										.append("</a></td>")
										.append("<td>").append(numberOfKeys).append("</td></tr>");

							} catch (IOException e) {
								e.printStackTrace();
							}
						});
			} catch (IOException e) {
				e.printStackTrace();
				res.status(500, "Internal Error - Listing Tables");
				return null;
			}

			html.append("</table></body></html>");
			res.type("text/html");
			return html.toString();
		});

		Server.get("/tables", (req, res) -> {

			String result = "";

			String next;
			for (Iterator iter = Table.tables.keySet().iterator(); iter.hasNext(); result = result + next + "\n") {
				next = (String) iter.next();
			}

			return result;

			// StringBuilder tablesList = new StringBuilder();
			//
			// // Append in-memory tables
			// System.out.println("==============kvs/Worker get
			// tables====================");
			// System.out.println("number of tables: "+ Table.tables.size());
			//
			// for (String tableName : Table.tables.keySet()) {
			// System.out.println("adding in memory: "+ tableName);
			// tablesList.append(tableName).append("\n");
			// }
			//
			// // Append persistent tables from the storage directory
			// try (Stream<Path> paths = Files.walk(Paths.get(storageDirectory), 1)) {
			// paths
			// .filter(Files::isDirectory)
			// .filter(path -> !path.equals(Paths.get(storageDirectory)))
			// .map(path -> path.getFileName().toString())
			// .forEach(tableName -> tablesList.append(tableName).append("\n"));
			// } catch (IOException e) {
			// e.printStackTrace();
			// res.status(500, "Internal Error - Listing Tables");
			// return null;
			// }
			//
			// // Setting content type as plain text
			// res.type("text/plain");
			// System.out.println("tablesList length: " +tablesList.length());
			// return tablesList.length() == 0 ? null : tablesList.toString();
		});

		Server.get("/view/:table", (req, res) -> {
			String tableName = req.params("table");
			String fromRow = req.queryParams("fromRow");

			if (tableName == null) {
				res.status(400, "Bad request");
				return "Bad request";
			}

			StringBuilder html = new StringBuilder("<html><body><h1>Table: ").append(tableName).append("</h1>");
			html.append("<table border='1'><tr><th>Row Key</th>");

			Map<String, Row> rowsMap = null;

			if (tableName.contains("pt-")) {
				rowsMap = Table.readRowsFromDirectory(tableName, storageDirectory);
			} else {
				rowsMap = Table.tables.get(tableName);
			}

			if (rowsMap == null) {
				res.status(404, "Table Not Found");
				return "Table Not Found";
			}

			List<String> rowKeys = new ArrayList<>(rowsMap.keySet());
			Collections.sort(rowKeys);

			int startIndex = 0;
			if (fromRow != null) {
				startIndex = Collections.binarySearch(rowKeys, fromRow);
				startIndex = startIndex < 0 ? ~startIndex : startIndex;
			}

			int endIndex = Math.min(startIndex + 10, rowKeys.size());

			Set<String> columns = new TreeSet<>();
			for (int i = startIndex; i < endIndex; i++) {
				columns.addAll(rowsMap.get(rowKeys.get(i)).columns());
			}
			columns.forEach(column -> html.append("<th>").append(column).append("</th>"));
			html.append("</tr>");

			for (int i = startIndex; i < endIndex; i++) {
				String rowKey = rowKeys.get(i);
				Row row = rowsMap.get(rowKey);
				html.append("<tr><td>").append(rowKey).append("</td>");
				columns.forEach(column -> html.append("<td>").append(row.get(column)).append("</td>"));
				html.append("</tr>");
			}

			html.append("</table>");
			if (endIndex < rowKeys.size()) {
				html.append("<a href='/view/").append(tableName).append("?fromRow=").append(rowKeys.get(endIndex))
						.append("'>Next</a>");
			}
			html.append("</body></html>");
			res.header("content-type", "text/html");
			return html.toString();
		});

		Server.put("/data/:table/:row/:column", (req, res) -> {
			String table = req.params("table");
			String rowKey = req.params("row");
			String column = req.params("column");

			if (table == null || rowKey == null || column == null) {
				res.status(400, "Bad request");
				return "Bad request";
			}

			String ifColumn = req.queryParams("ifcolumn");
			String equals = req.queryParams("equals");

			// create table if not exist
			Table.createTable(table, storageDirectory);
			Row row = Table.getRow(table, rowKey, storageDirectory);

			if (ifColumn != null && equals != null) {
				byte[] value = row.getBytes(ifColumn);
				if (value == null || !Arrays.equals(value, equals.getBytes())) {
					res.status(200, "OK");
					return "FAIL";
				}
			}

			// create row if not exist
			if (row == null) {
				row = new Row(rowKey);
			}
			row.put(column, req.bodyAsBytes());
			Table.putRow(table, rowKey, row, storageDirectory);

			res.status(200, "OK");
			return "OK";
		});

		Server.get("/data/:table/:row/:column", (req, res) -> {
			String table = req.params("table");
			String rowKey = req.params("row");
			String column = req.params("column");

			if (table == null || rowKey == null || column == null) {
				res.status(400, "Bad request");
				return "Bad request";
			}

			Row row = Table.getRow(table, rowKey, storageDirectory);

			if (row == null) {
				res.status(404, "Not Found");
				return "Table or Row not found";
			}

			byte[] value = row.getBytes(column);
			if (value == null) {
				res.status(404, "Not Found");
				return "Column not found";
			}

			res.bodyAsBytes(value);
			return null;
		});

		Server.get("/data/:table/:row", (req, res) -> {
			String table = req.params("table");
			String rowKey = req.params("row");

			if (table == null || rowKey == null) {
				res.status(400, "Bad request");
				return "Bad request";
			}

			Row row = Table.getRow(table, rowKey, storageDirectory);
			if (row != null) {
				res.status(200, "OK");
				res.type("text/plain");
				res.bodyAsBytes(row.toByteArray());
			} else {
				res.status(404, "Table/row not found");
			}
			return null;
		});

		Server.get("/data/:table", (req, res) -> {
			String tableName = req.params("table");
			String startRow = req.queryParams("startRow");
			String endRowExclusive = req.queryParams("endRowExclusive");

			res.type("text/plain");
			// ByteArrayOutputStream baos = new ByteArrayOutputStream();

			if (tableName == null) {
				res.status(400, "Bad request");
				return "Bad request";
			}
			// Map<String, Row> rowsMap = null;

			if (tableName.contains("pt-")) {
				// rowsMap = Table.readRowsFromDirectory(tableName, storageDirectory);
				File tableDirectory = new File(storageDirectory + File.separator + tableName);

				if (!tableDirectory.exists() || !tableDirectory.isDirectory()) {
					res.status(404, "Table not found");
					return "Table not found";
				}

				File[] files = tableDirectory.listFiles();
				if (files == null) {
					return null;
				}

				// ConcurrentMap<String, Row> rows = new ConcurrentHashMap<String, Row>();;
				for (File file : files) {
					try (FileInputStream fis = new FileInputStream(file)) {
						Row row = Row.readFrom(fis);
						if (row != null) {
							if ((startRow == null || row.key.compareTo(startRow) >= 0) &&
									(endRowExclusive == null || row.key.compareTo(endRowExclusive) < 0)) {
								res.write(row.toByteArray());
								res.write("\n".getBytes(StandardCharsets.UTF_8));
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} else {
				Map<String, Row> rowsMap = Table.tables.get(tableName);
				if (rowsMap == null) {
					res.status(404, "Table not found");
					return "Table not found";
				}
				Iterator<String> keyIterator = rowsMap.keySet().iterator();

				while (keyIterator.hasNext()) {
					String rowKey = keyIterator.next();
					if ((startRow == null || startRow.compareTo(rowKey) <= 0) &&
							(endRowExclusive == null || endRowExclusive.compareTo(rowKey) > 0)) {
						res.write(rowsMap.get(rowKey).toByteArray());
						res.write("\n".getBytes(StandardCharsets.UTF_8));
					}
				}
			}

			// if (rowsMap == null) {
			// res.status(404, "Table not found");
			// return "Table not found";
			// }

			// List<String> rowKeys = new ArrayList<>(rowsMap.keySet());
			// Collections.sort(rowKeys);
			//
			//
			//
			// for (String rowKey : rowKeys) {
			// if ((startRow == null || rowKey.compareTo(startRow) >= 0) &&
			// (endRowExclusive == null || rowKey.compareTo(endRowExclusive) < 0)) {
			//
			// Row row = rowsMap.get(rowKey);
			// baos.write(row.toByteArray());
			// baos.write("\n".getBytes(StandardCharsets.UTF_8));
			// }
			// }
			res.write("\n".getBytes(StandardCharsets.UTF_8));
			// res.bodyAsBytes(baos.toByteArray());
			return null;
		});

		Server.put("/data/:table/", (req, res) -> {
			String table = req.params("table");
			if (table == null) {
				res.status(400, "Bad request");
				return "Bad request";
			}
			// create table if not exist
			Table.createTable(table, storageDirectory);
			ByteArrayInputStream in = new ByteArrayInputStream(req.bodyAsBytes());

			while (true) {
				Row row = Row.readFrom(in);
				if (row == null) {
					return "OK";
				}

				Table.putRow(table, row.key, row, storageDirectory);
				res.status(200, "OK");
				return "OK";
			}
		});

		Server.get("/count/:table", (req, res) -> {
			String tableName = req.params("table");
			if (tableName == null) {
				res.status(400, "Bad request");
				return "Bad request";
			}

			int count = -1;
			if (tableName.contains("pt-")) {
				count = Table.countRows(tableName, storageDirectory);
			} else {
				Map<String, Row> rowsMap = Table.tables.get(tableName);
				if (rowsMap != null) {
					count = rowsMap.size();
				}
			}

			if (count == -1) {
				res.status(404, "Table not found");
				return "Table not found";
			}
			return count;
		});

		Server.put("/rename/:table", (req, res) -> {
			String oldName = req.params("table");
			String newName = req.body();

			if (oldName == null || newName == null) {
				res.status(400, "Bad request");
				return "Bad request";
			}

			if (!Table.tables.containsKey(oldName)
					&& !Files.exists(Paths.get(storageDirectory, KeyEncoder.encode(oldName)))) {
				res.status(404, "Table Not Found");
				return "Table Not Found";
			}

			if (Table.tables.containsKey(newName)
					|| Files.exists(Paths.get(storageDirectory, KeyEncoder.encode(newName)))) {
				res.status(409, "Name Already Exists");
				return "Name Already Exists";
			}

			if ((oldName.startsWith("pt-") && !newName.startsWith("pt-"))) {
				res.status(400, "Cannot convert a table from pt to in memory");
				return "Cannot convert a table from pt to in memory";
			}

			if (!oldName.startsWith("pt-")) {
				Map removedMap = Table.tables.remove(oldName);
				if (newName.startsWith("pt-")) {
					(new File(storageDirectory + File.separator + KeyEncoder.encode(newName))).mkdirs();
					Iterator iter = removedMap.keySet().iterator();

					while (iter.hasNext()) {
						String rowKey = (String) iter.next();
						Table.putRow(newName, rowKey, (Row) removedMap.get(rowKey), storageDirectory);
					}
				} else {
					Table.tables.put(newName, removedMap);
				}
			} else {
				Path oldPath = Paths.get(storageDirectory, KeyEncoder.encode(oldName));
				if (Files.exists(oldPath)) {
					Path newPath = Paths.get(storageDirectory, KeyEncoder.encode(newName));
					try {
						Files.move(oldPath, newPath);
					} catch (IOException e) {
						e.printStackTrace();
						res.status(500, "Internal Error- Rename");
						return "Internal Error- Rename";
					}
				}
			}
			res.status(200, "OK");
			return "OK";
		});

		Server.put("/delete/:table", (req, res) -> {
			String tableName = req.params("table");
			if (tableName == null) {
				res.status(400, "Bad request");
				return "Bad request";
			}

			if (!tableName.contains("pt-")) {
				if (Table.tables.remove(tableName) != null) {
					return "OK";
				} else {
					res.status(404, "Table Not Found");
					return "Table Not Found";
				}
			}

			Path tablePath = Paths.get(storageDirectory + File.separator + tableName);
			if (!Files.exists(tablePath)) {
				res.status(404, "Table Not Found");
				return "Table Not Found";
			}
			try {
				Files.walk(tablePath).map(Path::toFile).forEach(File::delete);
				Files.deleteIfExists(tablePath);
				res.status(200, "OK");
				return "OK";
			} catch (IOException e) {
				e.printStackTrace();
				res.status(500, "Internal Error- Delete");
				return "Internal Error- Delete";
			}
		});

	}

	public static String generateId() {
		Random random = new Random();
		StringBuilder id = new StringBuilder(5);
		for (int i = 0; i < 5; i++) {
			id.append((char) ('a' + random.nextInt(26)));
		}
		return id.toString();
	}
}
