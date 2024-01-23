package search.kvs;

import static search.webserver.Server.*;

import search.webserver.Server;

public class Coordinator extends search.generic.Coordinator {
    public Coordinator(int port) {
        registerRoutes();
        get("/", (req, res) -> {
            res.type("text/html");
            return "<html>" +
                    "<head><title>KVS Coordinator</title></head>" +
                    "<body>" +
                    "<h1>KVS Coordinator</h1>" +
                    clientTable() + // Assuming workerTable() returns a String of HTML
                    "</body>" +
                    "</html>";
        });
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Please provide a port number.");
            System.exit(1);
        }

        try {
            int port = Integer.parseInt(args[0]);

            if (port <= 0 || port > 65535) {
                System.out.println("Please provide a valid port number between 1 and 65535.");
                System.exit(1);
            }

            Server.port(port);
            new Coordinator(port);

        } catch (NumberFormatException e) {
            System.out.println("Please provide a valid integer for the port number.");
            System.exit(1);
        }
    }

}
