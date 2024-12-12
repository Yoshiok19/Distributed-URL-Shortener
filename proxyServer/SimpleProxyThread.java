import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class SimpleProxyThread extends Thread {
    private Socket client;
    private Socket server;
    ConsistentHashing consistentHashing;
    boolean replicate;

    public SimpleProxyThread(Socket client, ConsistentHashing consistentHashing, boolean replicate) {
        super();
		this.client = client;
        this.consistentHashing = consistentHashing;
        this.replicate = replicate;
    }

    public void handleRequest(String request, String cacheKey, String host, int port, OutputStream streamToClient, boolean shouldCache) throws IOException {
        try {
            // Attempt to open a connection to server
            server = new Socket(host, port); 
        } catch (IOException e) {
            PrintWriter out = new PrintWriter(streamToClient);
            out.print("Proxy server cannot connect to " + host + ":"+ port + ":\n" + e + "\n");
            out.flush();
            client.close();
            return;
        }
        
        // Get server streams.
        final InputStream streamFromServer = server.getInputStream();
        final OutputStream streamToServer = server.getOutputStream();
        // Send request
        PrintWriter p = new PrintWriter(streamToServer);
        p.println(request);
        p.flush();

        // Read the server's responses and pass them back to the client.
        byte[] reply = new byte[4096];
        int bytesRead;
        try {
            while ((bytesRead = streamFromServer.read(reply)) != -1) {
                streamToClient.write(reply, 0, bytesRead);
                streamToClient.flush();
            }

            if (SimpleProxyServer.CACHING && shouldCache && SimpleProxyServer.cache.size() < SimpleProxyServer.CACHE_LIMIT) {
                String res = new String(reply, StandardCharsets.UTF_8);
                String httpResponseCode = res.split(" ")[1];

                if (Objects.equals(httpResponseCode, "200") || Objects.equals(httpResponseCode, "307")) {
                    // Cache response only if successful
                    synchronized (this) {
                        SimpleProxyServer.cache.put(cacheKey, reply);
                    }
                }
            }
            
        } catch (IOException e) {
        }
        System.out.println("Success: " + request);
        // The server closed its connection to us, so we close our
        // connection to our client.
        streamToClient.close();
    }

    public void run() {
        try {
            final InputStream streamFromClient = client.getInputStream();
            final OutputStream streamToClient = client.getOutputStream();

            BufferedReader reader = new BufferedReader(new InputStreamReader(streamFromClient));
            String clientRequest = reader.readLine();
            System.out.println(clientRequest);
            
            String shortResource, longResource, httpVersion, hashNum, request;

            Pattern pput = Pattern.compile("^PUT\\s+/\\?short=(\\S+)&long=(\\S+)\\s+(\\S+)$");
            Matcher mput = pput.matcher(clientRequest);

            Pattern pget = Pattern.compile("^(\\S+)\\s+/(\\S+)\\s+(\\S+)$");
            Matcher mget = pget.matcher(clientRequest);

            if (mput.matches()) { // PUT request
                shortResource = mput.group(1);
                longResource = mput.group(2);
                httpVersion = mput.group(3);
                hashNum = "" + Math.abs(shortResource.hashCode()) % 120;
                request = "PUT /?short=" + shortResource + "&long=" + longResource + "&hashnum=" + hashNum + " HTTP/1.1";
                // Get nodes from hash
                ArrayList<Node> nodes = new ArrayList<>();
                nodes = consistentHashing.getNodesFromHash(shortResource);
                if (!nodes.isEmpty()) {
                    // Send to first server
                    handleRequest(request, shortResource, nodes.getFirst().host, nodes.getFirst().port, streamToClient, false);
                }

                // Replication
                Socket server2 = null;
                if (replicate && nodes.size() > 1) { // Can only replicate if there are more than 1 node
                    try {
                        server2 = new Socket(nodes.get(1).host, nodes.get(1).port);
                        final InputStream streamFromServer = server2.getInputStream();
                        final OutputStream streamToServer = server2.getOutputStream();
                        // Send request (Don't read response)
                        PrintWriter p = new PrintWriter(streamToServer);
                        p.println(request);
                        p.flush();
                        server2.close();
                    } catch (IOException e) {
                        if (server2 != null) {
                            server2.close();
                        }
                    }
                }
                
            } else if (mget.matches()){
                // GET Request
                shortResource=mget.group(2);
                httpVersion=mget.group(3);
                hashNum = "" + Math.abs(shortResource.hashCode()) % 120;
                request = "GET /" + shortResource + " HTTP/1.1";
                if (SimpleProxyServer.CACHING && SimpleProxyServer.cache.containsKey(shortResource)) { // Cache hit
                    System.out.println("CACHE HIT");
                    streamToClient.write(SimpleProxyServer.cache.get(shortResource));
                    streamToClient.flush();
                    return;
                }
                System.out.println("CACHE MISS");

                // Get nodes from hash
                ArrayList<Node> nodes = new ArrayList<>();
                nodes = consistentHashing.getNodesFromHash(shortResource);
                if (!nodes.isEmpty()) {
                    handleRequest(request, shortResource, nodes.getFirst().host, nodes.getFirst().port, streamToClient, true);
                }
            } else {
                // No match
                String res = "404 NO MATCH";
                PrintWriter p = new PrintWriter(streamToClient);
                p.println(res);
                p.flush();
                p.close();
            }

        } catch (Exception e) {
            System.err.println(e);
        } finally {
            try {
                if (server != null)
                    server.close();
                if (client != null)
                    client.close();
            } catch (IOException e) {
            }
        }
    }
}