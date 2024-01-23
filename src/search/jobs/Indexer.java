package search.jobs;

import search.external.PorterStemmer;
import search.flame.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Indexer {
    public static void run(FlameContext context, String[] args) throws Exception {
        FlameRDD data = context.fromTable("pt-crawl", row -> {
            if (row.get("url").contains("..") || row.get("url").length() > 100) {
                return null;
            }
            String page = row.get("page");
            // String extracted = removeTagsAndPunctuation(page).toLowerCase();
            return row.get("url") + "," + page;
        }, true);
        data.saveAsTable("pt-data");

        FlamePairRDD pairs = null;
        try {
            pairs = data.mapToPair(s -> {
                String[] parts = s.split(",", 2);
                FlamePair pair = new FlamePair(parts[0], parts[1]);
                return pair;
            }, true);
        } catch (Exception e) {
            System.out.println("Error catched in indexer");
        }
        pairs.saveAsTable("pt-pairs");

        // u, p pairs to w, u pairs
        FlamePairRDD wordUrlPairs = pairs.flatMapToPair(pair -> {
            String url = pair._1();
            String pageContent = pair._2();
            // String text = removeTagsAndPunctuation(pageContent).toLowerCase();
            //
            // if (text.trim().isEmpty()) {
            // return Collections.emptyList();
            // }

            List<String> words = new ArrayList<>();
            for (String s : pageContent.trim().split("\\s+")) {
                PorterStemmer stemmer = new PorterStemmer();
                stemmer.add(s.toCharArray(), s.length());
                stemmer.stem();
                words.add(stemmer.toString());
            }

            return words.stream().map(word -> new FlamePair(word, url)).collect(Collectors.toList());
        }, true);
        wordUrlPairs.saveAsTable("pt-wordurlpairs");

        FlamePairRDD invertedIndex = wordUrlPairs.foldByKey("", (urls, url) -> {
            if (!urls.isEmpty() && !urls.contains(url)) {
                return urls + "," + url;
            } else if (urls.isEmpty()) {
                return url;
            }
            return urls;
        }, true);
        invertedIndex.saveAsTable("pt-title");

    }

    public static String removeTagsAndPunctuation(String content) {
        String textFromTags = extractVisibleText(content);
        String noHtml = textFromTags.replaceAll("<[^>]*>", " ");
        return noHtml.replaceAll("[^a-zA-Z0-9\\s]", " ");
    }

    public static String extractVisibleText(String content) {

        content = content.replaceAll("(?s)<script.*?</script>", "");
        content = content.replaceAll("(?s)<style.*?</style>", "");

        StringBuilder visibleText = new StringBuilder();
        // extracting text from specific tags
        String[] tagPatterns = {
                "<title>(.*?)</title>",
                // "<p>(.*?)</p>",
                // "<span>(.*?)</span>",
                // "<div>(.*?)</div>",
                // "<h[1-6]>(.*?)</h[1-6]>",
                // "<a[^>]*>(.*?)</a>",
                // "<b>(.*?)</b>", "<strong>(.*?)</strong>",
                // "<i>(.*?)</i>", "<em>(.*?)</em>",
                // "<u>(.*?)</u>",
                // "<mark>(.*?)</mark>",
                // "<small>(.*?)</small>",
                // "<sub>(.*?)</sub>",
                // "<sup>(.*?)</sup>",
                // "<li>(.*?)</li>",
                // "<td>(.*?)</td>",
                // "<th>(.*?)</th>"
        };

        for (String pattern : tagPatterns) {
            Matcher matcher = Pattern.compile(pattern, Pattern.DOTALL).matcher(content);
            while (matcher.find()) {
                visibleText.append(matcher.group(1)).append(" ");
            }
        }

        return visibleText.toString();
    }
}