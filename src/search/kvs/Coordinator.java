package search.kvs;

import static search.webserver.Server.*;

import search.webserver.Server;

public class Coordinator extends search.generic.Coordinator {
    // Constructor initializes a web server with routes and behavior for requests.
    public Coordinator(int port) {
        registerRoutes();
        get("/", (req, res) -> {
            res.type("text/html");
            // Renders a simple HTML page for the root route.
            return "<html><head><title>KVS Coordinator</title></head><body><h1>KVS Coordinator</h1>" +
                    clientTable() + // Displays a table of clients (assumed functionality)
                    "</body></html>";
        });
    }

    // Entry point of the program. It sets up the Coordinator server on a specified
    // port.
    public static void main(String[] args) {
        // Ensures the port number is provided as a command-line argument.
        if (args.length < 1) {
            System.out.println("Please provide a port number.");
            System.exit(1);
        }

        try {
            int port = Integer.parseInt(args[0]);
            // Validates the port number range.
            if (port <= 0 || port > 65535) {
                System.out.println("Please provide a valid port number between 1 and 65535.");
                System.exit(1);
            }

            Server.port(port); // Sets the server to listen on the specified port.
            new Coordinator(port); // Starts the server.

        } catch (NumberFormatException e) {
            // Catches invalid port number format.
            System.out.println("Please provide a valid integer for the port number.");
            System.exit(1);
        }
    }
}
