package cis5550.frontend;

import cis5550.external.PorterStemmer;
import cis5550.kvs.KVSClient;

import static cis5550.webserver.Server.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Mainpage {
    // TODO: Change the ip part to the KVSClient instance
    private static KVSClient kvsClient = new KVSClient("127.0.0.1:8000");

    public static void main(String[] args) throws Exception {
        port(80);
        securePort(443);
        get("/", (req, res) -> {

            String filePath = "index.html";
            try {
                String content = new String(Files.readAllBytes(Paths.get(filePath)));
                res.type("text/html");
                return content;
            } catch (Exception e) {
                e.printStackTrace();
                return "Error loading page";
            }
        });
        get("/hello", (req, res) -> {
            return "Hello World!";
        });

        // broswer->http server->backend server(search keys in kvs table)->http server->broswer
        get("/search", (req, res) -> {
            String query = req.queryParams("query");

            // offset and limit
            String offsetParam = req.queryParams("offset");
            String limitParam = req.queryParams("limit");
            int offset = offsetParam != null ? Integer.parseInt(offsetParam) : 0;
            int limit = limitParam != null ? Integer.parseInt(limitParam) : 15;

            if (query == null || query.trim().isEmpty()) {
                res.status(400, "Bad request");
                return "Query parameter is missing";
            }

            String[] words = query.trim().toLowerCase().split("\\s+");
            for (int i = 0; i < words.length; i++) {
                words[i] = stemWord(words[i]);
            }

            Map<String, Double> urlScores = new HashMap<>();
            Map<String, String[]> pageDetails = new HashMap<>();

            for (String word : words) {
                // pt-title
                String urlString = fetchUrlsForWord(word);

                // if no URLs found, continue
                if (urlString.isEmpty()) {
                    continue;
                }

                // split the string by comma
                String[] urls = urlString.split(",");

                // pagerank and score
                for (String url : urls) {
                    double pagerank = fetchPageRank(url);

                    if (pagerank == 0.0) {
                        if (urls.length >= 10) {
                            continue;
                        }
                        pagerank = 0.15;
                    }
                    double titleScore = 1.0;
                    double finalScore = 0.3 * pagerank + 0.5 * titleScore;

                    String[] details = fetchPageDetails(url);
                    if (!details[0].isEmpty()) {
                        urlScores.put(url, urlScores.getOrDefault(url, 0.0) + finalScore);
                        pageDetails.put(url, details);
                    }
                }
            }

            for (String word : words) {
                // pt-tiidf
                double tfidf = fetchtfidf(word);
                if (tfidf == 0.0) {
                    continue;
                }
                double tiidfScore = 0.2 * tfidf;
                // update score
                for (String url : urlScores.keySet()) {
                    urlScores.put(url, urlScores.getOrDefault(url, 0.0) + tiidfScore);
                }
            }

            // If no URLs found, return no matched results
            if (urlScores.isEmpty()) {
                return "No matched pages found";
            }

            // sorting
            List<Map.Entry<String, Double>> sortedEntries = new ArrayList<>(urlScores.entrySet());
            sortedEntries.sort((e1, e2) -> e2.getValue().compareTo(e1.getValue()));


            // if offset is greater than the number of results, return nothing
            if (offset >= sortedEntries.size()) {
                return "";
            } else if (offset + limit > sortedEntries.size()) {
                limit = sortedEntries.size() - offset;
            }

            List<Map.Entry<String, Double>> limitedEntries = sortedEntries
                    .stream()
                    .skip(offset)
                    .limit(limit)
                    .toList();

            return formatSearchResults(limitedEntries, pageDetails);
        });
    }

    private static String fetchUrlsForWord(String word) {
        StringBuilder urls = new StringBuilder();
        HttpURLConnection con = null;
        try {
            kvsClient.numWorkers();
            String workerAddress = kvsClient.getWorkerAddress(kvsClient.workerIndexForKey(word));

            URL url = new URL("http://" + workerAddress + "/data/pt-title/" + word + "/acc");
            con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            // Set timeouts
            con.setConnectTimeout(10000);
            con.setReadTimeout(3000);

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                urls.append(inputLine);
            }
            in.close();
        } catch (Exception e) {

        } finally {
            if (con != null) {
                con.disconnect();
            }
        }
        return urls.toString();
    }

    private static double fetchPageRank(String url) {
        double pageRank = 0.0;
        HttpURLConnection con = null;
        try {
            kvsClient.numWorkers();
            String workerAddress = kvsClient.getWorkerAddress(kvsClient.workerIndexForKey(url));

            URL urlObj = new URL("http://" + workerAddress + "/data/pt-pageranks/" + url + "/rank");
            con = (HttpURLConnection) urlObj.openConnection();
            con.setRequestMethod("GET");

            // Set timeouts
            con.setConnectTimeout(10000);
            con.setReadTimeout(3000);

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            if ((inputLine = in.readLine()) != null) {
                pageRank = Double.parseDouble(inputLine);
            }
            in.close();

        } catch (Exception e) {

        } finally {
            if (con != null) {
                con.disconnect();
            }
        }
        return pageRank;
    }

    private static double fetchtfidf(String word) {
        double tfidfScore = 0.0;
        HttpURLConnection con = null;
        try {
            kvsClient.numWorkers();
            String workerAddress = kvsClient.getWorkerAddress(kvsClient.workerIndexForKey(word));

            URL urlObj = new URL("http://" + workerAddress + "/data/pt-tfidf/" + word + "/acc");
            con = (HttpURLConnection) urlObj.openConnection();
            con.setRequestMethod("GET");

            // Set timeouts
            con.setConnectTimeout(10000);
            con.setReadTimeout(3000);

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            if ((inputLine = in.readLine()) != null) {
                String[] parts = inputLine.split(",");
                if (parts.length > 1) {
                    tfidfScore = Double.parseDouble(parts[1]);
                }
            }
            in.close();

        } catch (Exception e) {

        } finally {
            if (con != null) {
                con.disconnect();
            }
        }
        return tfidfScore;
    }

    private static String fetchOriginalUrl(String urlKey) {
        StringBuilder result = new StringBuilder();
        HttpURLConnection con = null;
        try {
            kvsClient.numWorkers();
            String workerAddress = kvsClient.getWorkerAddress(kvsClient.workerIndexForKey(urlKey));

            URL url = new URL("http://" + workerAddress + "/data/pt-crawl/" + urlKey + "/url");
            con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            // Set timeouts
            con.setConnectTimeout(10000);
            con.setReadTimeout(3000);

            result = new StringBuilder();
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                result.append(inputLine);
            }
            in.close();
        } catch (Exception e) {

        } finally {
            if (con != null) {
                con.disconnect();
            }
        }

        return result.toString();
    }

    private static String[] fetchPageDetails(String urlKey) {
        String originalUrl = "";
        String title = "";
        String snippet = "";

        // original URL
        originalUrl = fetchOriginalUrl(urlKey);

        if (originalUrl.isEmpty()) {
            return new String[]{"", title, snippet};
        }
        if (!originalUrl.isEmpty()) {
            // page content
            String pageContent = fetchPageContent(urlKey);
            title = extractTitle(pageContent);
            snippet = extractSnippet(pageContent);
        }

        return new String[]{originalUrl, title, snippet};
    }

    private static String fetchPageContent(String urlKey) {
        StringBuilder pageContent = new StringBuilder();
        HttpURLConnection con = null;
        try {
            kvsClient.numWorkers();
            String workerAddress = kvsClient.getWorkerAddress(kvsClient.workerIndexForKey(urlKey));

            URL url = new URL("http://" + workerAddress + "/data/pt-crawl/" + urlKey + "/page");
            con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            // Set timeouts
            con.setConnectTimeout(10000);
            con.setReadTimeout(3000);

            pageContent = new StringBuilder();
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                if (inputLine.contains("<title>") || inputLine.contains("<p>")) {
                    pageContent.append(inputLine).append("\n");
                }
            }
            in.close();
        } catch (Exception e) {

        } finally {
            if (con != null) {
                con.disconnect();
            }
        }
        return pageContent.toString();
    }

    private static String extractTitle(String pageContent) {
        final String titleStartTag = "<title>";
        final String titleEndTag = "</title>";

        int start = pageContent.indexOf(titleStartTag);
        if (start == -1) {
            return "No title";
        }
        start += titleStartTag.length();
        int end = pageContent.indexOf(titleEndTag, start);
        if (end == -1) {
            return "No title";
        }
        return pageContent.substring(start, end).trim();
    }

    private static String extractSnippet(String pageContent) {
        final String pStartTag = "<p>";
        final String pEndTag = "</p>";
        StringBuilder snippetBuilder = new StringBuilder();
        int wordCount = 0;
        int start = pageContent.indexOf(pStartTag);

        while (start != -1 && wordCount < 50) {
            start += pStartTag.length();
            int end = pageContent.indexOf(pEndTag, start);
            if (end == -1) {
                // No end tag
                break;
            }
            String snippet = pageContent.substring(start, end).replaceAll("\\<.*?\\>", "").trim();
            String[] words = snippet.split("\\s+");

            for (String word : words) {
                if (wordCount >= 50) {
                    break;
                }
                snippetBuilder.append(word).append(" ");
                wordCount++;
            }

            start = pageContent.indexOf(pStartTag, end + pEndTag.length());
        }
        return snippetBuilder.toString().trim();
    }

    private static String formatSearchResults(List<Map.Entry<String, Double>> sortedEntries, Map<String, String[]> pageDetails) {
        StringBuilder builder = new StringBuilder();
        builder.append("<div class='search-results'>\n");

        for (Map.Entry<String, Double> entry : sortedEntries) {
            String url = entry.getKey();
            String[] details = pageDetails.get(url);
            String originalUrl = details[0];
            String title = details[1];
            String snippet = details[2];

            builder.append("<div class='search-result-item'>\n");
            builder.append("<a href='").append(originalUrl).append("'>").append(title).append("</a>\n");
            builder.append("<p>").append(snippet).append("</p>\n");
            builder.append("</div>\n");
        }

        builder.append("</div>\n");
        return builder.toString();
    }

    public static String stemWord(String word) {
        PorterStemmer ps = new PorterStemmer();
        ps.add(word.toCharArray(), word.length());
        ps.stem();
        return ps.toString();
    }

}
