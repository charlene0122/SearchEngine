package cis5550.jobs;

import cis5550.external.PorterStemmer;
import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;

import java.util.Arrays;
import java.util.Map;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static cis5550.tools.Hasher.hash;

public class TF_IDF {
    public static void run(FlameContext context, String[] args) throws Exception {
        // Implementation will go here
        FlameRDD rawRDD = context.fromTable("pt-crawl", (Row row) ->{
            if (row.get("url").length() < 100 && "200".equals(row.get("responseCode")) && row.get("contentType") != null && row.get("contentType").contains("text/html")) {
                return row.get("url") + "," + row.get("page");
            }else{
                return null;
            }
        },true);
        rawRDD.saveAsTable("pt-data");


        // Use mapToPair to convert to PairRDD
        FlamePairRDD pairRDD = rawRDD.mapToPair(s -> {
            String[] parts = s.split(",", 2);
            return new FlamePair(parts[0], parts[1]);
        },true);
        pairRDD.saveAsTable("pt-pairs");

//        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
//
//        // 安排任务每10秒执行一次
//        executor.scheduleAtFixedRate(() -> {
//            long totalMemory = Runtime.getRuntime().totalMemory(); // 总内存
//            long freeMemory = Runtime.getRuntime().freeMemory();   // 空闲内存
//            long usedMemory = totalMemory - freeMemory;            // 已用内存
//
//            System.out.println("Total Memory: " + totalMemory / (1024 * 1024) + " MB");
//            System.out.println("Free Memory: " + freeMemory / (1024 * 1024) + " MB");
//            System.out.println("Used Memory: " + usedMemory / (1024 * 1024) + " MB");
//            System.out.println("----------------------------------------");
//        }, 0, 10, TimeUnit.SECONDS);

        // Step 1: 计算TF值
        FlamePairRDD urlWordTfPairs = pairRDD.flatMapToPair(pair -> {
            double a = 0.4;
            String url = pair._1();
            String pageContent = pair._2();
            String textOnly = removeHTMLTagsAndPunctuation(pageContent).toLowerCase();

            if (textOnly.trim().isEmpty()) {
                return Collections.emptyList();
            }

            ArrayList<String> words = new ArrayList<>();
            for(String s : textOnly.trim().split("\\s+")){
                if(s.length() > 100) continue;
                words.add(stemWord(s));
            }

//            String[] words = textOnly.trim().split("\\s+");

            Map<String, Long> wordCounts = Arrays.stream(words.toArray(new String[0]))
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
//            long totalWords = words.length;

            long maxFreq = wordCounts.values().stream().max(Long::compare).orElse(0L);

            return wordCounts.entrySet().stream()
                    .map(e -> {
                        double adjustedTf = a + (1 - a) * e.getValue() / (double) maxFreq;
                        return new FlamePair(e.getKey(), url + "," + adjustedTf);
                    })
                    .collect(Collectors.toList());
        },true);
        urlWordTfPairs.saveAsTable("pt-tf");

        // Step 2: 计算每个词出现的次数
        FlamePairRDD wordDocumentCount = rawRDD.flatMapToPair(s -> {
            String[] parts = s.split(",", 2);
            String textOnly = removeHTMLTagsAndPunctuation(parts[1]).toLowerCase();

            if (textOnly.trim().isEmpty()) {
                return Collections.emptyList();
            }

            Set<String> uniqueWords = new HashSet<>();
            for (String str : textOnly.trim().split("\\s+") ){
                if(str.length() > 100) continue;
                uniqueWords.add(stemWord(str));
            }

            return uniqueWords.stream()
                    .map(word -> new FlamePair(word, "1"))
                    .collect(Collectors.toList());
        },true);
        wordDocumentCount.saveAsTable("pt-wordDocumentCount");

        FlamePairRDD wordDocCount = wordDocumentCount.foldByKey("0", (count1, count2) -> String.valueOf(Integer.parseInt(count1) + Integer.parseInt(count2)),true);
        wordDocCount.saveAsTable("pt-wordDocCount");

        // 计算总文档数
        long totalDocuments = rawRDD.count();

        // Step 3: 计算IDF值
        FlamePairRDD wordIdfValues = wordDocCount.flatMapToPair(pair -> {
            String word = pair._1();
            double idf = Math.log(totalDocuments / Double.parseDouble(pair._2()));
            return Collections.singletonList(new FlamePair(word, String.valueOf(idf)));
        },true);
        wordIdfValues.saveAsTable("pt-wordIdfValues");

//        urlWordTfPairs = context.fromTable("pt-tf", row -> row.get("value")).mapToPair(s -> {
//            String[] parts = s.split(",", 2);
//            return new FlamePair(parts[0], parts[1]);
//        });

        // Step 3: 合并TF和IDF值，计算TF-IDF

        FlamePairRDD joined = urlWordTfPairs.join(wordIdfValues,true);
        joined.saveAsTable("pt-joined");

        FlamePairRDD wordTfIdfPairs = joined.flatMapToPair(pair -> {
            String word = pair._1();
            String combinedData = pair._2(); // 获取合并后的数据，格式为 "url,TF,IDF"

            // 解析合并后的数据
            String[] parts = combinedData.trim().split(",");
            if (parts.length < 3) {
                // 数据格式不正确，应处理错误或跳过
                return Collections.emptyList();
            }
            String url = parts[0];
            double tf = Double.parseDouble(parts[1]);
            double idf = Double.parseDouble(parts[2]);
            double tfIdfValue = tf * idf;
            return Collections.singletonList(new FlamePair(word, url + "," + tfIdfValue));
        },true);
        wordTfIdfPairs.saveAsTable("pt-wordTfIdfPairs");

        FlamePairRDD invertedIndex = wordTfIdfPairs.foldByKey("", (accum, url) -> {
            if (accum.isEmpty()) return url;
            return accum + ", " + url;
        },true);

        invertedIndex.saveAsTable("pt-tfidf-index");
    }

    private static String removeHTMLTagsAndPunctuation(String content) {
        content = extractVisibleText(content);
        String noHtml = content.replaceAll("<[^>]*>", " ");
        return noHtml.replaceAll("[^a-zA-Z0-9\\s]", " ");
    }

    private static String extractVisibleText(String content) {
        content = content.replaceAll("(?s)<script.*?</script>", "");
        content = content.replaceAll("(?s)<style.*?</style>", "");

        StringBuilder visibleText = new StringBuilder();
        // extracting text from specific tags
        String[] tagPatterns = {
                "<title>(.*?)</title>",
                "<p>(.*?)</p>",
//                "<span>(.*?)</span>",
//                "<div>(.*?)</div>",
//                "<h[1-6]>(.*?)</h[1-6]>",
//                "<a[^>]*>(.*?)</a>",
//                "<b>(.*?)</b>", "<strong>(.*?)</strong>",
//                "<i>(.*?)</i>", "<em>(.*?)</em>",
//                "<u>(.*?)</u>",
//                "<mark>(.*?)</mark>",
//                "<small>(.*?)</small>",
//                "<sub>(.*?)</sub>",
//                "<sup>(.*?)</sup>",
//                "<li>(.*?)</li>",
//                "<td>(.*?)</td>",
//                "<th>(.*?)</th>"
        };

        for (String pattern : tagPatterns) {
            Matcher matcher = Pattern.compile(pattern, Pattern.DOTALL).matcher(content);
            while (matcher.find()) {
                visibleText.append(matcher.group(1)).append(" ");
            }
        }

        return visibleText.toString();
    }

        public static String stemWord(String word) {
        PorterStemmer ps = new PorterStemmer();
        ps.add(word.toCharArray(), word.length());
        ps.stem();
        return ps.toString();
    }
}
