package cis5550.webserver;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ResponseImpl implements Response {

	private PrintWriter writer;
	private int statusCode = 200;
	private String reasonPhrase = "OK";
	private String reqMethod;
	private Map<String,String> headers = new HashMap<String,String>();
	private byte body[] = null;
	private boolean hasWritten = false;

	public ResponseImpl(PrintWriter writer, String reqMethod) {
		this.writer = writer;
		this.reqMethod = reqMethod;
	}

	public String body() {
		return new String(body, StandardCharsets.UTF_8);
	}

	public byte[] bodyAsBytes() {
		return body;
	}

	public boolean hasWritten() {
		return hasWritten;
	}

	@Override
	public void body(String body) {
		if (!hasWritten) {
			this.body = body.getBytes();
		}
	}

	@Override
	public void bodyAsBytes(byte[] bodyArg) {
		if (!hasWritten) {
			this.body = bodyArg;
		}

	}

	@Override
	public void header(String name, String value) {
		headers.put(name.toLowerCase(), value);
	}

	@Override
	public void type(String contentType) {
		this.headers.put("content-type", contentType);
	}

	@Override
	public void status(int statusCode, String reasonPhrase) {
		this.statusCode = statusCode;
		this.reasonPhrase = reasonPhrase;
	}

	public void writeHeader() {
		if (!hasWritten) {
			hasWritten = true;
			writer.write("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n");

			for (Map.Entry<String, String> entry : headers.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();
				writer.write(key + ": " + value + "\r\n");
			}

			if (!headers.containsKey("content-type")) {
				writer.write( "content-type: " + "text/plain" + "\r\n");
			}

			writer.write("\r\n");
			writer.flush();
		}
	}


	@Override
	public void write(byte[] b) throws Exception {
		if (!hasWritten) {
			writeHeader();
		}
		if (!reqMethod.equals("HEAD")){
			writer.write(new String(b, StandardCharsets.UTF_8));
			writer.flush();
		}
	}


	@Override
	public void redirect(String url, int responseCode) {
		this.statusCode = responseCode;
		header("Location", url);

	}

	@Override
	public void halt(int statusCode, String reasonPhrase) {
		this.status(statusCode, reasonPhrase);
		try {
			writeHeader();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public String toString(){
		int b =  body ==null? 0: body.length;
		return "=======RESPONSE===============================\n"
				+ " " + statusCode + " " + reasonPhrase + " headers: " + headers
				+ " BODY: \n"
				+ "=======start of body=========\n"
				+ b + "\n"
				+ "=======end of body=========\n"
				+ "\n\n";
	}
}
