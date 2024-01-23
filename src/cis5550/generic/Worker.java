package cis5550.generic;

import cis5550.tools.Logger;

import java.io.IOException;
import java.net.URL;

public class Worker{
	private static final long INTERVAL = 5000;
	public static Logger logger = Logger.getLogger(Worker.class);

    protected static void startPingThread(String ipPort, String workerId, int port) {
    	new Thread(() -> {
			while (true) {
				try {
					Thread.sleep(INTERVAL);
					URL url = new URL("http://" + ipPort + "/ping?id=" + workerId + "&port=" + port);
					url.getContent();
				} catch (InterruptedException e) {
					e.printStackTrace();
					break;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}).start();
    }
    
    
}
