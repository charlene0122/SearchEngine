package cis5550.generic;

import cis5550.tools.Logger;
import cis5550.webserver.Server;

import java.util.*;

import static cis5550.webserver.Server.*;

public class Coordinator {
	protected static Map<String, WorkerNode> workers = new HashMap<>();
    private static final Logger logger = Logger.getLogger(Coordinator.class);

	public static Vector<String> getWorkers() {
		Vector<String> results = new Vector<>();
		checkExpired();
        workers.forEach((id, worker) ->
			results.add(worker.ip()+":"+worker.port())
        );
		return results;
    }
	
	public static void checkExpired() {
//		List<String> expired = new ArrayList<>();
//		long currTime = System.currentTimeMillis();
//		for (Map.Entry<String, WorkerNode> set : workers.entrySet()) {
//			if (currTime - set.getValue().getLastPing() > 600000) { //10min
//                expired.add(set.getKey());
//			}
//		}
//		 for (String worker : expired) {
//         	workers.remove(worker);
//         }
	}

    public static String clientTable() {
    	checkExpired();
    	StringBuilder builder = new StringBuilder("<html><body><table>");
        builder.append("<tr><th>ID</th><th>IP</th><th>Port</th><th>Link</th></tr>"); 
        workers.forEach((id, worker) -> {
            String link = "http://" + worker.ip() + ":" + worker.port() + "/";
            builder.append("<tr>")
                   .append("<td>").append(id).append("</td>")
                   .append("<td>").append(worker.ip()).append("</td>")
                   .append("<td>").append(worker.port()).append("</td>")
                   .append("<td><a href='").append(link).append("'>")
                   .append("Link to Worker ").append(id).append("</a></td>")
                   .append("</tr>");
        });
        builder.append("</table></body></html>");
        return builder.toString();
    }

    public static void registerRoutes() {
       
    	Server.get("/ping", (req, res) -> {
            String workerId = req.queryParams("id");
            String port = req.queryParams("port"); 
            
            // Check for missing id and/or port and return 400 error if any is missing
            if (workerId == null || workerId.isEmpty() || port == null || port.isEmpty()) {
                res.status(400, "Bad Request"); 
                return "Bad Request: Missing id and/or port"; 
            }

            if(workerId.equals("!!")){
                workerId = req.ip();
            }

            // Adding or updating the worker data
            WorkerNode worker;
            if (workers.containsKey(workerId + port)) {
            	worker = workers.get(workerId + port);
            } else {
	            worker = new WorkerNode(workerId);
            }
            worker.ip(req.ip());
        	worker.port(port);
        	worker.refreshLastPing();
        	workers.put(workerId + port, worker);
//            logger.info("ping " +req.ip() + " " + port + "time: " + System.currentTimeMillis());

            res.status(200, "OK"); 
            return "OK"; 
        });

        get("/workers", (req, res) -> {
        	checkExpired();
            res.type("text/plain");
            StringBuilder builder = new StringBuilder();
            builder.append(workers.size()).append("\n"); 
            workers.forEach((id, worker) -> 
                builder.append(id).append(",").append(worker.ip()).append(":").append(worker.port()).append("\n")
            );
            return builder.toString(); // Return the constructed response string
        });
    }

}
