package cis5550.webserver;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

public class SessionImpl implements Session {

	private static final String SESSION_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	private static final int SESSION_ID_LENGTH = 20;
	private static final SecureRandom random = new SecureRandom();

	private String sessionId;
	private long creationTime;
	private long lastAccessedTime;
	private int maxActiveInterval;
	private Map<String, Object> attributes;

	public SessionImpl(String id) {
		this.sessionId = id;
		long currTime = System.currentTimeMillis();
		this.maxActiveInterval = 300;
		this.creationTime = currTime;
		this.lastAccessedTime = currTime;
		this.attributes = new HashMap<>();
	}

	@Override
	public String id() {
		return sessionId;
	}

	@Override
	public long creationTime() {
		return creationTime;
	}

	@Override
	public long lastAccessedTime() {
		return lastAccessedTime;
	}

	public void setLastAccessedTime(long time) {
		this.lastAccessedTime = time;
	}

	public int maxActiveInterval() {
		return this.maxActiveInterval;
	}

	@Override
	public void maxActiveInterval(int seconds) {
		this.maxActiveInterval = seconds;
	}

	@Override
	public void invalidate() {
		maxActiveInterval = 0;
	}

	@Override
	public Object attribute(String name) {
		return attributes.get(name);
	}

	@Override
	public void attribute(String name, Object value) {
		attributes.put(name, value);
	}

	public static String generateSessionId() {
		StringBuilder sessionId = new StringBuilder(SESSION_ID_LENGTH);
		for (int i = 0; i < SESSION_ID_LENGTH; i++) {
			int index = random.nextInt(SESSION_CHARS.length());
			sessionId.append(SESSION_CHARS.charAt(index));
		}
		return sessionId.toString();
	}

}
