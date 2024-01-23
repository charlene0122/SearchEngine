package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public class Crawler {
    public static Logger logger = Logger.getLogger(Crawler.class);
    // Serialize an object to a byte array
    private static byte[] serialize(Serializable obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {

            oos.writeObject(obj);
            return bos.toByteArray();

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public static void run(FlameContext context, String[] args){
        KVSClient kvs = context.getKVS();

        if(args.length >= 1){
            context.output("OK");
            List<String> seedURLs = Arrays.stream(args).toList();
            seedURLs = seedURLs.stream().map(Crawler::normalizeSeedURL).toList();
            int iteration = 0;
            FlameRDD urlQueue;

            try {
                String queueTableName = new String(kvs.get("pt-last-queue", "key", "lastqueue"));
                iteration =Integer.valueOf(queueTableName.split("-")[2])+1;
                urlQueue = context.fromTable(queueTableName, r->{
                        return r.get("value");
                    }, true);

                //optimize for ec2
                if(urlQueue.count()>20000){
                    Vector<String> urls = urlQueue.take(20000);
                    urlQueue = context.parallelize(urls);
                }
                logger.info("    queueTable exists");

            } catch (Exception e) {
                logger.info("    queueTable does not exist, crawling from seeds instead");
                try {
                    urlQueue = context.parallelize(seedURLs);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }


            // start crawling
            try {

                //while the urlQueue is not empty
                while(urlQueue.count() >0){

                    logger.info("                              " );
                    logger.info("    ===================start crawl===================");
                    logger.info("    *urlQueue: " + urlQueue.count());
                    logger.info("    ===================start crawl===================");

                    // iterate each url in the urlQueue
                    FlameRDD.StringToIterable lambda = (String url)->{

                        // new crawled urls to return
                        List<String> newURLs= new ArrayList<>();
                        logger.info("                              " );
                        logger.info("                              " );
                        logger.info("    step1===process url: " + url );

                        //check if url has been crawled, if yes skip current url
                        if(!kvs.existsRow("pt-crawl", Hasher.hash(url))) {

                            //check robots.txt to make sure the url could be crawled
                            Map<String, List<String>> rulesMap = checkRobotsText(kvs, url);
                            boolean crawlAllowed = checkRules(rulesMap, url);
                            logger.info("    step3===crawl allow? " + crawlAllowed);

                            if (crawlAllowed) {

                                // get the interval time and check if delay time met
                                float interval = Float.parseFloat(rulesMap.get("crawl-delay").get(0));
                                boolean delayValid = checkCrawlDelay(kvs, url, (int) interval);

                                //if the url meet delay requirement, crawl it
                                if (delayValid) {

                                    // if head results and get results do not have exception errors, return the new URLs
                                    String[] headResults = sendHEAD(kvs, url);
                                    if (headResults != null) {
                                        List<String> getResults = sendGET(kvs, url, headResults, newURLs);
                                        if (getResults != null) {
                                            newURLs = getResults;

                                        }
                                    }
                                    // don't crawl it, save for next time
                                } else {
                                    logger.info("    Crawls delays unmet, save for next time to crawl: " + url);
                                    newURLs.add(url);
                                }
                            }
                        }
                        logger.info("    last step===return extracted urls: " + newURLs.size());
                        logger.info("                              " );
                        logger.info("                              " );
                        return newURLs;

                    };

                    FlameRDD nextURLQueue = urlQueue.flatMap(lambda, true);

                    //save the urls in the current queue for crash recovery mechanism
                    String queueTableName = "pt-queue-" + iteration;
                    kvs.put("pt-last-queue", "key", "lastqueue", queueTableName.getBytes());

                    //optimize for ec2
                    FlameRDD savedTable = nextURLQueue;
                    if(savedTable.count()>20000){
                        Vector<String> urls = savedTable.take(20000);
                        savedTable = context.parallelize(urls);
                    }
                    savedTable.saveAsTable(queueTableName);

                    iteration++;
                    urlQueue = nextURLQueue;
                    logger.info("    ===================end of iteration summary===================");
                    logger.info("    *nextURLQueue: " + nextURLQueue.count());
                    logger.info("    *urlQueue: " + urlQueue.count());
                    logger.info("    *savedTable: " + savedTable.count());
                    logger.info("    ===================end of iteration summary===================");
                }

            } catch (Exception e) {
                logger.error("error: "+ e);
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }else{
            context.output("input seed URL error");
        }
    }

    // make HEAD request first to check html and response 200
    public static String[] sendHEAD(KVSClient kvs, String url) {
        URL httpURL = null;
        try {
            logger.info("    step5===send HEAD: start of method");

            httpURL = new URL(url);
            HttpURLConnection.setFollowRedirects(false);
            HttpURLConnection connection = (HttpURLConnection) httpURL.openConnection();
            connection.setConnectTimeout(1000);
            connection.setReadTimeout(1000);
            connection.setRequestMethod("HEAD");
            connection.setRequestProperty("User-Agent", "cis5550-crawler");
            Map<String,List<String>> headers = connection.getHeaderFields();

            // Create a new map with lowercase keys
//            Map<String, List<String>> headersLowerCase = new HashMap<>();
//            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
//                if(entry.getKey()!=null){
//                    headersLowerCase.put(entry.getKey().toLowerCase(), entry.getValue());
//                }else{
//                    headersLowerCase.put(entry.getKey(), entry.getValue());
//                }
//            }
//            logger.info("    step5===send HEAD: connection headers: " + headersLowerCase.toString());


            //put url
            kvs.put("pt-crawl", Hasher.hash(url), "url", url.getBytes());

            //put response code
            String responseCode = String.valueOf(connection.getResponseCode());
            kvs.put("pt-crawl", Hasher.hash(url), "responseCode", responseCode.getBytes());
            kvs.put("pt-crawl", Hasher.hash(url), "url", url.getBytes());
            logger.info("    step5===send HEAD: url: " + url + ", " + "response code: " + responseCode);


            //put content length
            String contentLength = "0";
            if(headers.containsKey("content-length")){
                contentLength = headers.get("content-length").get(0);
                kvs.put("pt-crawl", Hasher.hash(url), "length", contentLength.getBytes());
            }
            if(headers.containsKey("Content-Length")){
                contentLength = headers.get("Content-Length").get(0);
                kvs.put("pt-crawl", Hasher.hash(url), "length", contentLength.getBytes());
            }
            // put content type
            String contentType = "";
            if(headers.get("content-type")!=null && headers.get("content-type").get(0) !=null){
                contentType = headers.get("content-type").get(0);
                kvs.put("pt-crawl", Hasher.hash(url), "contentType", contentType.getBytes());
            }
            if(headers.get("Content-Type")!=null && headers.get("Content-Type").get(0) !=null){
                contentType = headers.get("Content-Type").get(0);
                kvs.put("pt-crawl", Hasher.hash(url), "contentType", contentType.getBytes());
            }
            // Close the connection and return head results
            connection.disconnect();
            return new String[]{responseCode, contentType, contentLength};

        } catch (MalformedURLException e) {
            logger.error("malformed URL");
        } catch (ProtocolException e) {
            logger.error("protocol error");
        } catch (java.net.SocketTimeoutException e){
            logger.error("connection timeout");
        } catch (IOException e) {
            logger.error("io error");
        }catch (java.lang.ClassCastException e){
            logger.error("httpURLconnection cast type error");
        }
        return null;
    }


    //make the GET request
    public static List<String> sendGET(KVSClient kvs, String url, String[] headResults, List<String> newURLs){
        String headResponseCode = headResults[0];
        String contentType = headResults[1];
        int contentLength = Integer.valueOf(headResults[2]);

        // establish connection
        URL httpURL = null;
        try {
            httpURL = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) httpURL.openConnection();
            connection.setConnectTimeout(2000);
            connection.setReadTimeout(2000);


            // if 200 and content type is html and the content is smaller than 5mb
            if( contentType.contains("text/html") && headResponseCode.equals("200") && contentLength < 5000000) {
                connection = (HttpURLConnection) httpURL.openConnection();
                connection.setRequestMethod("GET");
                connection.setRequestProperty("User-Agent", "cis5550-crawler");

                //put the response code
                int responseCode = connection.getResponseCode();

                if(responseCode ==200){
                    //get body in the request
                    logger.info("    step6===send GET: start to get html content");
                    BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    String inputLine;
                    StringBuilder responseBuilder = new StringBuilder();
                    while ((inputLine = reader.readLine()) != null) {
                        responseBuilder.append(inputLine + "\n");
                    }
                    String content = responseBuilder.toString();


                    // put content in page column
                    kvs.put("pt-crawl", Hasher.hash(url), "page", content.getBytes());

                    logger.info("    step6===send GET: have put html content to pt-crawl, byte array length: " + content.getBytes().length);

                    connection.getInputStream().close();
                    connection.disconnect();

                    //extract urls
                    newURLs = extractURLs(content);
                    if(newURLs.size()> 11){ // optimize for ec2 instance speed, smaller exploration
                        newURLs = newURLs.subList(0,10);
                    }
                    newURLs = newURLs.stream().map(o -> normalizeURL(url, o)).collect(Collectors.toList());
                    logger.info("    step6===send GET: extracted normalized and filtered urls: " + newURLs.size());

                    }
                    return newURLs;

            }
            //handle redirect
            else if (headResponseCode.equals("301") || headResponseCode.equals("302")  || headResponseCode.equals("303")  ||headResponseCode.equals("307")  ||headResponseCode.equals("308") ) {
                String redirectURL = connection.getHeaderField("Location");
                if(redirectURL!=null){
                    String nextURL = Crawler.normalizeURL(url, redirectURL);
                    if(nextURL!=null){
                        newURLs.add(nextURL);
                        logger.info("    send GET: base url: " + url + ",  redirection, wait for another round" + Crawler.normalizeURL(url, redirectURL) + "code: " + headResponseCode);
                    }
                }
                return newURLs;
            }
            // other cases return empty list
            // other response code, content type not valid, content length exceeding 10 mb
            else {
                logger.info("    send GET: not valid: response Code: " + headResponseCode + "Content Type: " + contentType + "Content Length: " + contentLength);
            }
            // Close the connection
            connection.disconnect();
            return newURLs;

        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        } catch (ProtocolException e) {
            logger.error("protocol error");
        }catch (java.net.SocketTimeoutException e){
            logger.error("connection timeout");
        } catch (IOException e) {
            logger.error("io error");
        }catch (java.lang.ClassCastException e){
            logger.error("httpURLconnection cast type error");
        }
        return null;
    }


    ////////////////////////////////////////////////////robot text//////////////////////////////////////////////////////


    public static Map<String, List<String>> checkRobotsText(KVSClient kvs, String url){
        String hostDomain = URLParser.parseURL(url)[1];
        String hostDomainHashedKey = Hasher.hash(hostDomain);
        Map<String, List<String>> rulesMap = new HashMap<>();
        try {
            // if the site has been searched for robot text
            if(kvs.existsRow("pt-host", hostDomainHashedKey)){
                String responseCode =  new String(kvs.get("pt-host", hostDomainHashedKey, "responseCode"), "UTF-8");
                // robot text found
                if(responseCode.equals("200")){
                    logger.info("    step2===get robotText from pt-host, success");
                    String text =  new String(kvs.get("pt-host", hostDomainHashedKey, "robotsText"), "UTF-8");
                    rulesMap = parseRobotsText(text);
                    return rulesMap;
                }
                // no robot text, should not visit url
                else{
                    logger.info("    step2===get robotText from pt-host, not exist");
                    return null;
                }
                // if this is the first time to search for robot text
            }else{
                String robotURL = URLParser.parseURL(url)[0] +"://"+ URLParser.parseURL(url)[1] + ":" + URLParser.parseURL(url)[2] +  "/robots.txt";
                URL httpURL = new URL(robotURL);
                HttpURLConnection connection = (HttpURLConnection) httpURL.openConnection();
                connection.setRequestMethod("GET");
                connection.setRequestProperty("User-Agent", "cis5550-crawler");
                connection.setConnectTimeout(1000);
                connection.setReadTimeout(1000);


                int responseCode = connection.getResponseCode();
                kvs.put("pt-host", hostDomainHashedKey, "responseCode", String.valueOf(responseCode).getBytes());
                kvs.put("pt-host", hostDomainHashedKey, "hostURL", hostDomain.getBytes());

                if(responseCode ==200) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    String inputLine;
                    StringBuilder responseBuilder = new StringBuilder();
                    while ((inputLine = reader.readLine()) != null) {
                        responseBuilder.append(inputLine + "\n");
                    }
                    String response = responseBuilder.toString();
                    reader.close();
                    kvs.put("pt-host", hostDomainHashedKey, "robotsText", response.getBytes());
                    logger.info("    step2===first time get robotText, success");
                    rulesMap = parseRobotsText(response);
                    return rulesMap;
                }else{
                    logger.info("    step2===first time get robotText, fail");
                }
            }
        } catch (ProtocolException e) {
            logger.error("protocol error: "+ e);
        } catch (MalformedURLException e) {
            logger.error("malformed url error: "+ e);
        } catch (FileNotFoundException e) {
            logger.error("file not found error: "+ e);
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            logger.error("unsupported encoding error: "+ e);
        } catch (IOException e) {
            logger.error("io error: "+ e);
        }
        return null;
    }

    public static Map<String, List<String>> parseRobotsText(String text){
        Map<String, List<String>> ret = new HashMap<>();

        text = text.toLowerCase();
        if(text.indexOf("user-agent: cis5550-crawler\n")!=-1){
            int pos = text.indexOf("user-agent: cis5550-crawler\n");
            text = text.substring(pos + "user-agent: cis5550-crawler\n".length());
            if(text.indexOf("user-agent:")!=-1){
                int endPos = text.indexOf("user-agent:");
                text = text.substring(0, endPos);
            }
            match(text, ret);

        } else if (text.indexOf("user-agent: *\n")!=-1) {
            int pos = text.indexOf("user-agent: *\n");
            text = text.substring(pos + "user-agent: *\n".length());
            if(text.indexOf("user-agent:")!=-1){
                int endPos = text.indexOf("user-agent:");
                text = text.substring(0, endPos);
            }
            match(text, ret);
        }
        // default 1 sec crawl delay if not specified
        if(!ret.containsKey("crawl-delay")){
            List<String> lst = new ArrayList<>(){{add("1000");}};
            ret.put("crawl-delay", lst);
        }
//        logger.info("    rules:" + ret);
        return  ret;
    }

    public static void match(String text, Map<String, List<String>> rulesMap){
        Pattern pattern = Pattern.compile("crawl-delay: [0-9.]*(?=\n)");
        Matcher matcher = pattern.matcher(text);
        if(matcher.find()){
            String match = matcher.group();
            String delay = String.valueOf(Float.parseFloat(match.split(": ")[1]) * 1000);
            List<String> lst = rulesMap.getOrDefault("crawl-delay", new ArrayList<String>());
            lst.add(delay);
            rulesMap.put("crawl-delay",lst);
        }

        Pattern pattern2 = Pattern.compile("\\ballow: .*(?=\n)");
        Matcher matcher2 = pattern2.matcher(text);
        while(matcher2.find()){
            String match2 = matcher2.group();
            String[] allowURLSplits = match2.split(": ");

            //corner case of allow: \n
            if(allowURLSplits.length ==1){
                return;
            }else{
                String allowURL = allowURLSplits[1];
                List<String> lst = rulesMap.getOrDefault("allow", new ArrayList<String>());
                lst.add(allowURL);
                rulesMap.put("allow", lst);
            }
        }

        Pattern pattern3 = Pattern.compile("disallow: .*(?=\n)");
        Matcher matcher3 = pattern3.matcher(text);
        while(matcher3.find()){
            String match3 = matcher3.group();
            String[] disallowURLSplits = match3.split(": ");

            //corner case of disallow: \n
            if(disallowURLSplits.length ==1){
                return;
            }else{
                String disallowURL = disallowURLSplits[1];
                List<String> lst = rulesMap.getOrDefault("disallow", new ArrayList<String>());
                lst.add(disallowURL);
                rulesMap.put("disallow", lst);
            }
        }
    }



    public static boolean checkRules(Map<String, List<String>> rulesMap, String url) {
        if(rulesMap==null){
            return false;
        }else{
            List<String> allowPrefixes = rulesMap.getOrDefault("allow", null);
            List<String> disallowPrefixes = rulesMap.getOrDefault("disallow", null);
            String path = URLParser.parseURL(url)[3];
            if(disallowPrefixes!=null){
                for(String disallowPrefix:disallowPrefixes ){
                    if(path.startsWith(disallowPrefix)){
                        return false;
                    }
                }
            }
            if (allowPrefixes!=null){
                for(String allowPrefix : allowPrefixes ) {
                    if (path.startsWith(allowPrefix)) {
                        return true;
                    }
                }
                return true;
            }else{
                return true;
            }
        }
    }


    public static boolean checkCrawlDelay(KVSClient kvs, String url, int interval){
        String hostDomain = URLParser.parseURL(url)[1];
        String hostDomainHashedKey = Hasher.hash(hostDomain);

    try {
        if(kvs.existsRow("pt-host", hostDomainHashedKey)){
            byte[] value = kvs.get("pt-host", hostDomainHashedKey, "timeStamp");
            if(value!=null){
                String timeStamp =  new String(value, "UTF-8");
                long timeStampLong = Long.parseLong(timeStamp);
                if( System.currentTimeMillis() - timeStampLong > interval){ // more than 1 sec
                    kvs.put("pt-host", hostDomainHashedKey, "timeStamp", String.valueOf(System.currentTimeMillis()).getBytes());
                    logger.info("    step4===check crawl delay: update time to" + String.valueOf(System.currentTimeMillis()));
                    return true;
                }else{
                    return false;
                }
            }else{
                kvs.put("pt-host", hostDomainHashedKey, "timeStamp", String.valueOf(System.currentTimeMillis()).getBytes());
                logger.info("    step4===check crawl delay: create time stamp" + String.valueOf(System.currentTimeMillis()));
                return true;
            }
        }else{
            kvs.put("pt-host", hostDomainHashedKey, "timeStamp", String.valueOf(System.currentTimeMillis()).getBytes());
            logger.info("    step4===check crawl delay: create time stamp" + String.valueOf(System.currentTimeMillis()));
            return true;
        }
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
}





////////////////////////////////////////////////////url Extraction//////////////////////////////////////////////////////


    //looking for anchor and href pattern
    // Examples:
    // <a target="_blank" href="blah.html">
    // <a href="blah.html">
    // <link rel="stylesheet" type="text/css" href="styles.css">
    public static List<String> extractURLs(String pageContent){
        HashSet<String> ret = new HashSet<>();
        Pattern pattern = Pattern.compile("<a\\s+[^>]*href\\s*=[^>]*>"); // find <a ... href= ...>
        Matcher matcher = pattern.matcher(pageContent);

        while (matcher.find()) {
            Pattern pattern2 = Pattern.compile("href\\s*=[\'\"][^>\\s]*[\'\"]\\s*"); // find href= url
            Matcher matcher2 = pattern2.matcher(matcher.group());
            if(matcher2.find()){

                Pattern pattern3 = Pattern.compile("[\'\"][^>\\s]*[\'\"]\\s*"); // find url
                Matcher matcher3 = pattern3.matcher(matcher2.group());
                if(matcher3.find()){

                    String nextURL = matcher3.group();
                    nextURL = nextURL.replace("\"", "").replace("\'", "").trim(); // remove the url quotation marks, white space

//                    // html entity encoding and url coding
//                    Pattern p = Pattern.compile("&#x([0-9A-Fa-f]+);");
//                    Matcher m = p.matcher(nextURL);
//                    nextURL = m.replaceAll(matchResult -> {
//                        String hexCode = matchResult.group(1);
//                        int codePoint = Integer.parseInt(hexCode, 16);
//                        return String.valueOf((char) codePoint);
//                    });
//
//                    nextURL = nextURL.replaceAll("&amp;", "&");
//                    nextURL = nextURL.replaceAll("&quot;", "\"");
//                    nextURL = nextURL.replaceAll("&apos;", "\'");
//
//                    try {
//                        nextURL = URLDecoder.decode(nextURL, StandardCharsets.UTF_8.toString());
//                    }catch (UnsupportedEncodingException e) {
//                        logger.warn("extracted url UnsupportedEncodingException error: nextURL: " + nextURL);
//                    }catch(java.lang.IllegalArgumentException e){
//                        logger.warn("extracted url IllegalArgumentException error: nextURL: " + nextURL);
//
//                    }

                    //filter out dynamic links like ${xxx}
                    if(!nextURL.contains("{") && !nextURL.contains("}")){
                        ret.add(nextURL);
                    }
                }
            }
        }
        return ret.stream().toList();
    }


    // normalize seed url, add missing port number and truncate the #tags in the path
    public static String normalizeSeedURL(String seedURL){
        String[] components = URLParser.parseURL(seedURL); // protocol, domain, port, path
        String protocol = components[0];
        String domain = components[1];
        String port = components[2];
        String path = components[3]; //if no, will be "/"

        if(port == null){
            if(protocol.equals("http")){
                port = "80";
            } else if (protocol.equals("https")) {
                port = "443";
            }
        }
        if(path.indexOf("#")!= -1){
            path = path.substring(0, path.indexOf("#"));
        }else if(path =="/"){
            path = "/";
        }
        return protocol+ "://" + domain + ":" + port + path;
    }

    public static String normalizeURL(String baseURL, String url){
        String[] baseComponents = URLParser.parseURL(baseURL); // protocol, domain, port, path
        String baseProtocol = baseComponents[0];
        String baseDomain = baseComponents[1];
        String basePort = baseComponents[2];
        String basePath = baseComponents[3];

        String[] components = URLParser.parseURL(url); // protocol, domain, port, path
        String protocol = components[0];
        String domain = components[1];
        String port = components[2];
        String path = components[3];


        // relative path ,relative to the base url. will return null if the relative path is wrong
        if(domain==null && protocol ==null && port ==null){
            protocol = baseProtocol;
            domain = baseDomain;
            port = basePort;

            if(path.startsWith("../")){
                int idx = basePath.lastIndexOf("/");
                if(idx == -1){
                    logger.error("relative path error, base url: "+ baseURL + ", url: " + url);
                    return baseURL;
                }else{
                    basePath = basePath.substring(0, idx);

                    while(path.indexOf("../")!=-1){
                        path  = path.substring(path.indexOf("../") + 3);
                        int idx2 = basePath.lastIndexOf("/");
                        if(idx2==-1){
                            logger.error("relative path error, base url: "+ baseURL + ", url: " + url);
                            return baseURL;
                        }else{
                            basePath = basePath.substring(0, idx2);
                        }
                    }
                    path = basePath +"/" + path;
                }
            }else if(path.startsWith("#")){
                path = basePath;
            }else if(!path.startsWith("/")){
                path = basePath.substring(0, basePath.lastIndexOf("/")) + "/" + path;
            }

            //truncate #xx if any remains
            if(path.indexOf("#")!=-1){
                path = path.substring(0, path.indexOf("#"));
            }
        }

        // link to the external host
       if (domain!=null && protocol !=null && port ==null) {
            if(protocol.equals("http")){
                port = "80";
            } else if (protocol.equals("https")) {
                port = "443";
            }
       }
       return protocol+ "://" + domain + ":" + port + path;
    }
}
