package cis5550.webserver;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import javax.net.ServerSocketFactory;
import javax.net.ssl.*;
import java.security.*;


public class Server implements Runnable {
	private static final int POOL_SIZE = 100;
	static Server server = null;
	static Boolean flag = false;

	private static int port = 80;
	private static int httpsPort = -1;
	static boolean running = false;

	static String host = "default";
	static Map<String, String> staticDir = new HashMap<>();
	static Map<String, List<RoutePattern>> routeTable = new HashMap<>();
	static Map<String, Session> sessionMap = new HashMap<>();
	BlockingQueue<Socket> queue = new LinkedBlockingQueue<>();

	public Server() {
		startSessionCleanupThread();
	}

	public void run() {
		Thread.currentThread().setName("server");
		String pwd = "secret";
		KeyStore keyStore;

		try {
			ServerSocket serverSocket = new ServerSocket(port);

			// handle https
			keyStore = KeyStore.getInstance("JKS");
			keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
			KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
			keyManagerFactory.init(keyStore, pwd.toCharArray());
			SSLContext sslContext = SSLContext.getInstance("TLS");
			sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
			ServerSocketFactory factory = sslContext.getServerSocketFactory();
			if (httpsPort > 0) {
			}

			running = true;
			for (int i = 0; i < POOL_SIZE; i++) {
				final int id = i;
				new ClientHandler(id, queue, this).start();

			}

			if (httpsPort >= 0) {
				ServerSocket serverSocketTLS = factory.createServerSocket(httpsPort);
				new Thread("TTLS") { public void run() { serverLoop(serverSocketTLS);  } }.start();
			}
			serverLoop(serverSocket);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	void serverLoop(ServerSocket socket) {
		while (true) {
			try {
				Socket connection = socket.accept();
				queue.put(connection);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void startSessionCleanupThread() {
		new Thread(() -> {
			while (true) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				long currentTime = System.currentTimeMillis();
				Set<String> toBeDeleted = new HashSet<>();

				for (Map.Entry<String, Session> set : sessionMap.entrySet()) {
					SessionImpl session = (SessionImpl)set.getValue();
					if ((currentTime - session.lastAccessedTime()) > session.maxActiveInterval()) {
						toBeDeleted.add(set.getKey());
						session.invalidate();
					}
				}
				for(String id : toBeDeleted) {
					sessionMap.remove(id);
				}

			}
		}).start();
	}

	public static class staticFiles {
		public static void location(String s) {
			server.launch();
			server.setDirectory(s);
		}
	}

	void setDirectory(String dir) {
		this.staticDir.put(host, dir);
	}

	class RoutePattern {
		String method;
		String path;
		Route route;

		RoutePattern(String method, String path, Route route) {
			this.method = method;
			this.path = path;
			this.route = route;
		}
	}

	public void addRoute(String method, String path, Route route) {
		RoutePattern newRoute = new RoutePattern(method, path, route);
		routeTable.putIfAbsent(host, new ArrayList<>());
		routeTable.get(host).add(newRoute);
	}

	public static void get(String s, Route r) {
		launch();
		server.addRoute("GET", s, r);
	}

	public static void post(String s, Route r) {
		launch();
		server.addRoute("POST", s, r);
	}

	public static void put(String s, Route r) {
		launch();
		server.addRoute("PUT", s, r);
	}

	public static void port(int num) {
		port = num;
	}
	public static void securePort(int num) {
		httpsPort = num;
	}

	public static void launch() {
		if (server == null) {
			server = new Server();
		}
		if (!flag) {
			flag = true;
			new Thread(server).start();
		}
	}
}
