package search.generic;

public class WorkerNode {
	private String id;
	private String ip;
	private String port;
	private long lastPing;

	public WorkerNode(String id) {
		this.id(id);
	}

	public long getLastPing() {
		return lastPing;
	}

	public void refreshLastPing() {
		this.lastPing = System.currentTimeMillis();
	}

	public String ip() {
		return ip;
	}

	public void ip(String ip) {
		this.ip = ip;
	}

	public String id() {
		return id;
	}

	public void id(String id) {
		this.id = id;
	}

	public String port() {
		return port;
	}

	public void port(String port) {
		this.port = port;
	}

	public String toString() {
		return id + ip + port + lastPing;
	}
}
