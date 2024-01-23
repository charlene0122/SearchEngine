package search.webserver;

import java.util.*;

import java.net.*;
import java.nio.charset.*;

// Provided as part of the framework code

class RequestImpl implements Request {
    String method;
    String url;
    String protocol;
    InetSocketAddress remoteAddr;
    Map<String, String> headers;
    Map<String, String> queryParams;
    Map<String, String> params;
    byte bodyRaw[];
    Server server;

    RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String, String> headersArg,
            Map<String, String> queryParamsArg, Map<String, String> paramsArg, InetSocketAddress remoteAddrArg,
            byte bodyRawArg[], Server serverArg) {
        method = methodArg;
        url = urlArg;
        remoteAddr = remoteAddrArg;
        protocol = protocolArg;
        headers = headersArg;
        queryParams = queryParamsArg;
        params = paramsArg;
        bodyRaw = bodyRawArg;
        server = serverArg;
    }

    @Override
    public String requestMethod() {
        return method;
    }

    public void setParams(Map<String, String> paramsArg) {
        params = paramsArg;
    }

    @Override
    public int port() {
        return remoteAddr.getPort();
    }

    @Override
    public String url() {
        return url;
    }

    @Override
    public String protocol() {
        return protocol;
    }

    @Override
    public String contentType() {
        return headers.get("content-type");
    }

    @Override
    public String ip() {
        return remoteAddr.getAddress().getHostAddress();
    }

    @Override
    public String body() {
        return new String(bodyRaw, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] bodyAsBytes() {
        return bodyRaw;
    }

    @Override
    public int contentLength() {
        return bodyRaw.length;
    }

    @Override
    public String headers(String name) {
        return headers.get(name.toLowerCase());
    }

    @Override
    public Set<String> headers() {
        return headers.keySet();
    }

    @Override
    public String queryParams(String param) {
        return queryParams.get(param);
    }

    @Override
    public Set<String> queryParams() {
        return queryParams.keySet();
    }

    @Override
    public String params(String param) {
        return params.get(param);
    }

    @Override
    public Map<String, String> params() {
        return params;
    }

    @Override
    public Session session() {
        String sessionId = null;

        // check if request contains a cookie
        if (headers.containsKey("cookie")) {
            sessionId = headers.get("cookie").split("=")[1];
            // check if cookie is contained in the map
            if (Server.sessionMap.containsKey(sessionId)) {
                SessionImpl session = (SessionImpl) Server.sessionMap.get(sessionId);
                // check if session has expired
                if (session.id() != null) {
                    long currentTime = System.currentTimeMillis();
                    if ((currentTime - session.lastAccessedTime()) > session.maxActiveInterval()) {
                        session.invalidate();
                    } else {
                        // if not expired, update last accessed time
                        session.setLastAccessedTime(currentTime);
                    }
                }

                // if expired, create a new session
                if (session.maxActiveInterval() == 0) {
                    Server.sessionMap.put(sessionId, new SessionImpl(sessionId));
                }
            }
        }

        if (sessionId == null || !Server.sessionMap.containsKey(sessionId)) {
            sessionId = SessionImpl.generateSessionId();
            SessionImpl newSession = new SessionImpl(sessionId);
            headers.put("set-cookie", sessionId);
            Server.sessionMap.put(sessionId, newSession);
        }

        return Server.sessionMap.get(sessionId);
    }

    public String toString() {
        int b = bodyRaw == null ? 0 : bodyRaw.length;
        return "=======REQUEST===============================\n"
                + method + " " + url + " " + protocol + " headers: " + headers + " queryParams: " + queryParams
                + " BODY: \n"
                + "=======start of body=========\n"
                + b + "\n"
                + "=======end of body=========\n"
                + "\n\n";
    }

}
