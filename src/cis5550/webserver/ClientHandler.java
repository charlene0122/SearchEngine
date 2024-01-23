package cis5550.webserver;

import cis5550.kvs.Worker;
import cis5550.tools.Logger;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;


public class ClientHandler extends Thread {

	private BlockingQueue<Socket> queue;
	private Server server;
	final Set<String> ALLOWEDMETHOD = new HashSet<>(Arrays.asList("GET", "PUT", "POST", "HEAD"));
	final String SPLITTER = "\r\n\r\n";
	public static Logger logger = Logger.getLogger(ClientHandler.class);



	public ClientHandler(int id, BlockingQueue<Socket> queue, Server server) {
		super("worker_" + id);
		this.queue = queue;
		this.server = server;
	}

	public void run() {
		Socket clientSocket = null;
		while (true) {
			try {
				clientSocket = this.queue.take();
				handleClient(clientSocket);
				clientSocket.close();
			} catch (Exception e) {
				e.printStackTrace();
				try {
					clientSocket.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
		}
	}

	public void handleClient(Socket clientSocket) {

		try {
			InputStream in = clientSocket.getInputStream();
			OutputStream out = clientSocket.getOutputStream();
			PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(out)), true);

			InetSocketAddress remoteAddr = (InetSocketAddress) clientSocket.getRemoteSocketAddress();

			StringBuilder sb = new StringBuilder();
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();

			byte[] buf = new byte[10000];
			int b = 0;

			while((b = in.read(buf)) != -1) {

				buffer.write(buf, 0, b);
				String current = new String(buf, 0, b);
				sb.append(current);


				while (true) {
					if (sb.indexOf(SPLITTER) == -1) {
						break;
					}

					RequestImpl request = generateRequest(sb, buffer, remoteAddr, in);
					ResponseImpl response = new ResponseImpl(writer, request.method);

					response.header("Connection", "close");

					boolean requestSent = checkError(request, writer);

					String host = "default";
					if (request.headers.containsKey("host")) {
						if (Server.routeTable.get(request.headers.get("host").split(":")[0]) != null) {
							host = request.headers.get("host").split(":")[0];
						}
					}

					for (Server.RoutePattern route : Server.routeTable.get(host)) {
						if (requestSent) break;
						HashMap<String, String> params = new HashMap<>();
						if (routeMatches(route, request.url, request.method, params)) {
							request.setParams(params);
							Object ob;
							try {
								ob = route.route.handle(request, response);
								if (ob != null) {
									response.body(ob.toString());
								}
								int length = response.bodyAsBytes() == null ? 0 : response.bodyAsBytes().length;
								response.header("content-length", String.valueOf(length));

								if (request.headers().contains("set-cookie")) {
									String sessionId = request.headers.get("set-cookie");
									response.header("set-cookie", "SessionID="+sessionId);
								}

								response.writeHeader();

								if (response.bodyAsBytes() != null) {
									response.write(response.bodyAsBytes());
								}
								requestSent = true;
							} catch(Exception e) {
								sendResponse(writer,500, "Internal Server Error");
								e.printStackTrace();
							}
							break;
						}
					}

					if (!requestSent && Server.staticDir.get(host) != null) {
						String fileName = Server.staticDir.get(host) + request.url;
						File file = new File(fileName);
						if (file.exists()) {
							if (file.canRead()) {
								if (request.method.equals("PUT") || request.method.equals("POST")) {
									sendResponse(writer, 405, "Not Allowed");
									requestSent = true;
								} else {
									String[] fileType = request.url.split("\\.");

									if (fileType.length == 2) {
										String contentType = switch (fileType[1]) {
											case "jpg", "jpeg" -> "image/jpeg";
											case "txt" -> "text/plain";
											case "html" -> "text/html";
											default -> "application/octet-stream";
										};
										response.type(contentType);
										long fileSize = file.length();
										if (request.method.equals("GET")) {
											byte[] fileContent = new byte[(int)fileSize];
											try {
												FileInputStream inputStream = new FileInputStream(file);
												inputStream.read(fileContent);
												response.bodyAsBytes(fileContent);
												response.header("content-length", String.valueOf(fileSize));
												response.write(response.bodyAsBytes());
												requestSent = true;
												inputStream.close();
											} catch (Exception e) {
												sendResponse(writer, 500, "Internal Server Error");
												requestSent = true;
											}
										}
									}
								}

							} else {
								sendResponse(writer, 403, "Forbidden");
							}
						} else {
							sendResponse(writer, 404, "Not Found");
						}
					}
					out.flush();
				}
			}
			clientSocket.close();
		} catch (Exception e) {
			e.printStackTrace();
			try {
				clientSocket.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

	}
	public void sendResponse(PrintWriter writer, int statusCode, String reasonPhrase) {
		writer.write("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\nContent-Length: " +
				(4 + reasonPhrase.length()) + "\r\n\r\n" + statusCode + " " + reasonPhrase);
		writer.flush();
	}

	public boolean routeMatches(Server.RoutePattern route, String reqPath, String method, Map<String, String> parameters) {
		if (!route.method.equals(method)) {
			if (!method.equals("HEAD") || !route.method.equals("GET")) {
				return false;
			}
		}
		String[] routeParts = route.path.split("/");
		String[] pathParts = reqPath.split("/");

		if (routeParts.length != pathParts.length) {
			return false;
		}
		for (int i = 1; i < routeParts.length; ++i) {
			if (routeParts[i].charAt(0) == ':') {
				parameters.put(routeParts[i].substring(1), URLDecoder.decode(pathParts[i], StandardCharsets.UTF_8));
			} else if (!routeParts[i].equals(pathParts[i])) {
				return false;
			}
		}
		return true;
	}

	public RequestImpl generateRequest(StringBuilder sb, ByteArrayOutputStream buffer, InetSocketAddress remoteAddr, InputStream in) throws IOException {
		byte[] bytes = buffer.toByteArray();
		buffer.reset();
		HashMap<String, String> headers = new HashMap<>();
		String reqMethod = null;
		String reqPath = null;
		String httpVersion = null;
		byte[] bodyAsBytes = null;
		int contentLength = -1;

		int endOfHeader = sb.indexOf(SPLITTER);
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(new ByteArrayInputStream(sb.substring(0, endOfHeader).getBytes())));
		sb.delete(0, endOfHeader + 4);
		buffer.write(bytes, endOfHeader + 4, bytes.length - endOfHeader - 4);

		String[] requestLine = reader.readLine().split(" ");
		if (requestLine.length == 3) {
			reqMethod = requestLine[0];
			reqPath = requestLine[1];
			httpVersion = requestLine[2];
		}

		String headerLine = reader.readLine();
		while (headerLine != null) {
			String[] parts = headerLine.split(" ", 2);
			if (parts.length == 2) {
				String key = parts[0].toLowerCase().substring(0, parts[0].length() - 1);
				headers.put(key, parts[1]);
			}
			headerLine = reader.readLine();
		}

		if (headers.containsKey("content-length")) {
			contentLength = Integer.parseInt(headers.get("content-length"));
			byte[] buf = new byte[10000];
			int b = 0;
			while (contentLength > buffer.size() && (b = in.read(buf)) != -1) {
				buffer.write(buf, 0, b);
				String current = new String(buf, 0, b);
				sb.append(current);
			}

			//transfer the body content from buffer to bodyAsBytes
			bodyAsBytes = new byte[contentLength];
			bytes = buffer.toByteArray();
			buffer.reset();
			for (int i = 0; i < contentLength; i++) {
				bodyAsBytes[i] = bytes[i];
			}
			sb.delete(0, contentLength);

			//write the remaining bytes [bytes-body] back to buffer
			buffer.write(bytes, contentLength, bytes.length - contentLength);
		}

		// extract queries from url
		HashMap<String, String> queryParams = new HashMap<>();
		if (reqPath != null && reqPath.indexOf('?') >= 0) {
			String urlQuery = reqPath.substring(reqPath.indexOf('?') + 1);
			String[] queries = urlQuery.split("&");
			for (String s : queries) {
				String[] terms = s.split("=");
				if (terms.length == 2) {
					queryParams.put(terms[0], java.net.URLDecoder.decode(terms[1], StandardCharsets.UTF_8));
				}
			}
			reqPath = reqPath.substring(0, reqPath.indexOf('?'));
		}

		// extract queries from request body
		if (headers.containsKey("content-type") &&
				headers.get("content-type").equals("application/x-www-form-urlencoded") &&
				bodyAsBytes != null) {
			String[] queries = new String(bodyAsBytes, StandardCharsets.UTF_8).split("&");
			for (String s : queries) {
				String[] terms = s.split("=");
				if (terms.length == 2) {
					queryParams.put(terms[0], java.net.URLDecoder.decode(terms[1], StandardCharsets.UTF_8));
				}
			}
		}

		//create request object
		RequestImpl request = new RequestImpl(reqMethod, reqPath, httpVersion, headers, queryParams, null, remoteAddr , bodyAsBytes, server);
		return request;
	}

	public boolean checkError(RequestImpl request, PrintWriter writer) {
		if (request.method == null || request.url == null || request.protocol == null || !request.headers.containsKey("host")) {
			sendResponse(writer, 400, "Bad Request");
			return true;
		}


		if (!request.protocol.equals("HTTP/1.1") && !request.protocol.equals("HTTP/1.0")) {
			sendResponse(writer, 505, "HTTP Version Not Supported");
			return true;
		}

		if (!ALLOWEDMETHOD.contains(request.method)) {
			sendResponse(writer, 501, "Not Implemented");
			return true;
		}

		if (request.url.contains("..")) {
			sendResponse(writer, 403, "Forbidden");
			return true;
		}

		return false;
	}
}
    
		
	   
	    
