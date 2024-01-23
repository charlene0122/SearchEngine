package search.jobs;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import search.flame.FlameContext;
import search.flame.FlamePair;
import search.flame.FlamePairRDD;
import search.flame.FlameRDD;
import search.tools.Hasher;
import search.tools.URLParser;

public class PageRank {

    public static void run(FlameContext context, String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Wrong input argument. Please provide convergence threshold");
            System.exit(1);
        }

        // step 3: lead the data to pagerank
        FlameRDD crawledData = context.fromTable("pt-crawl", row -> row.get("url") + "," + row.get("page"), true);
        crawledData.saveAsTable("pt-data");
        FlamePairRDD stateTable = crawledData.mapToPair(s -> {
            String[] values = s.split(",", 2);
            String url = values[0];
            String page = values[1];

            List<String> extractedUrls = extractUrls(page);

            StringBuilder normalizedUrls = new StringBuilder();
            for (String rawUrl : extractedUrls) {
                String processedUrl = normalizeUrl(rawUrl, url);
                if (processedUrl != null) {
                    String hashedUrl = Hasher.hash(processedUrl);
                    if (normalizedUrls.length() > 0) {
                        normalizedUrls.append(",");
                    }
                    normalizedUrls.append(hashedUrl);
                }
            }

            return new FlamePair(Hasher.hash(url), "1.0,1.0," + normalizedUrls.toString());
        }, true);

        stateTable.saveAsTable("pt-pair");

        double threshold = Double.parseDouble(args[0]);
        double maxRankChange;

        do {

            // step 4: compute the transfer table
            FlamePairRDD transferTable = stateTable.flatMapToPair(pair -> {
                String url = pair._1();
                String[] values = pair._2().split(",", 3);
                double rc = Double.parseDouble(values[0]);
                List<String> links = new ArrayList<>();

                if (values.length >= 1) {
                    String concatenatedLinks = values[2];
                    links = Arrays.asList(concatenatedLinks.split(","));
                }

                double computedRank = links.isEmpty() ? 0 : 0.85 * rc / links.size();

                List<FlamePair> transferedRank = new ArrayList<>();
                links.forEach(link -> {
                    if (!link.trim().isEmpty()) {
                        transferedRank.add(new FlamePair(link, String.valueOf(computedRank)));
                    }
                });
                transferedRank.add(new FlamePair(url, "0.0"));
                return transferedRank;

            }, true);
            transferTable.saveAsTable("pt-transfer");

            // step 5: aggregate the transfers
            FlamePairRDD newRanks = transferTable.foldByKey("0.0", (acc, rank) -> {
                double sumRank = Double.parseDouble(acc);
                double currRank = Double.parseDouble(rank);
                return String.valueOf(sumRank + currRank);
            }, true);

            newRanks.saveAsTable("pt-newranks");

            // step 6: update the state table
            FlamePairRDD joinedTable = stateTable.join(newRanks, true);
            joinedTable.saveAsTable("pt-joinedtable");

            FlamePairRDD updatedStateTable = joinedTable.flatMapToPair(pair -> {
                String url = pair._1();
                String[] values = pair._2().split(",");

                double accumulatedRank = Double.parseDouble(values[values.length - 1]);
                double newRank = accumulatedRank + 0.15;

                if (values.length >= 3) {
                    String links = String.join(",", Arrays.copyOfRange(values, 2, values.length - 1));
                    String newValue = newRank + "," + values[0] + "," + links;
                    return Collections.singletonList(new FlamePair(url, newValue));
                } else {
                    return Collections.emptyList();
                }
            }, true);

            updatedStateTable.saveAsTable("pt-updatedstatetable");
            // step 7: compute difference and check for convergence
            FlamePairRDD rankChanges = updatedStateTable.flatMapToPair(pair -> {
                String url = pair._1();
                String[] ranks = pair._2().split(",");

                double change = Double.parseDouble(ranks[1]) - Double.parseDouble(ranks[0]);
                String changeInRank = String.valueOf(Math.abs(change));

                return Collections.singletonList(new FlamePair(url, changeInRank));
            }, true);
            rankChanges.saveAsTable("pt-rankchanges");

            maxRankChange = rankChanges.foldByKey("0.0", (acc, rank) -> {
                double sumChange = Double.parseDouble(acc);
                double currChange = Double.parseDouble(rank);
                return String.valueOf(Math.max(sumChange, currChange));
            }, true).collect().stream()
                    .mapToDouble(pair -> Double.parseDouble(pair._2()))
                    .max()
                    .orElse(0.0);

            stateTable = updatedStateTable;

        } while (maxRankChange > threshold);

        // step 8: save the results
        stateTable.flatMapToPair(pair -> {
            String url = pair._1();
            String[] values = pair._2().split(",");
            double rank = Double.parseDouble(values[0]);
            context.getKVS().put("pt-pageranks", url, "rank", String.valueOf(rank));
            return Collections.emptyList();
        }, true);

    }

    public static List<String> extractUrls(String page) {
        List<String> urls = new ArrayList<>();

        Pattern pattern = Pattern.compile("<a\\s+(?:[^>]*?\\s+)?href=\"([^\"]*)\"",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(page);

        while (matcher.find()) {
            String url = matcher.group(1);
            if (url.startsWith("#") || url.trim().isEmpty()) {
                continue;
            }
            urls.add(url);
        }

        return urls;
    }

    public static String normalizeUrl(String rawUrl, String baseUrl) {
        try {
            String[] baseParts = URLParser.parseURL(baseUrl);
            String[] rawParts = URLParser.parseURL(rawUrl);

            String protocol = baseParts[0];
            String host = baseParts[1];
            String port = baseParts[2];
            String path = rawParts[3];

            if (rawParts[0] == null) {
                if (!path.startsWith("/")) {
                    path = new URL(new URL(baseUrl), rawUrl).getPath();
                }
                if (port == null) {
                    port = baseParts[2];
                }
            } else {
                protocol = rawParts[0];
                host = rawParts[1];
                port = rawParts[2];
            }

            path = new URI(path).normalize().getPath();

            if (!"http".equalsIgnoreCase(protocol) && !"https".equalsIgnoreCase(protocol)) {
                return null;
            }

            if (path.matches(".*\\.(jpg|jpeg|gif|png|txt)$")) {
                return null;
            }

            if (port == null || port.isEmpty()) {
                port = "http".equalsIgnoreCase(protocol) ? "80" : ("https".equalsIgnoreCase(protocol) ? "443" : port);
            }

            return new URL(protocol, host, Integer.parseInt(port), path).toString();

        } catch (MalformedURLException | URISyntaxException | NumberFormatException e) {
            return null;
        }
    }

}
