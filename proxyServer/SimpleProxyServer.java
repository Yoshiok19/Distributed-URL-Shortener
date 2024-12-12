import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class Node {
    public final String host;
    public final int port;
    public boolean alive = true;

    public final String processId;

    public Node(String host, int port, String processId) {
        this.host = host;
        this.port = port;
        this.processId = processId;
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            return true;
        } else {
            Node other = (Node) obj;
            return (Objects.equals(this.host, other.host)) && (this.port == other.port);
        }
    }

}

class ConsistentHashing {
    private NavigableMap<Integer, Node> hashRing;
    private ArrayList<Node> nodes;
    private int hashNum;
    private int replicationFactor;
    private int vnodes;

    ConsistentHashing(ArrayList<Node> nodes, int hashNum, int replicationFactor, int vnodes) {
        this.nodes = nodes;
        this.hashNum = hashNum;
        this.replicationFactor = replicationFactor;
        this.vnodes = vnodes;

        this.hashRing = new TreeMap<Integer,Node>();
        int numVNodes = this.nodes.size() * vnodes; // # of virtual nodes to add to ring

        if (numVNodes == 0) {
            return;
        }

        int range = hashNum / numVNodes;
        int key = range;
        for (int i = 0; i < numVNodes; i++) { // Evenly distribute virtual nodes on ring
            int index = i % this.nodes.size();
            if (index == 0) {
                this.hashRing.put(key, this.nodes.get(index));
            } else if (index == 1) {
                this.hashRing.put(key, this.nodes.get(index));
            } else {
                this.hashRing.put(key, this.nodes.get(index));
            }
            key += range;
        }
    }

    // Return the two nodes responsible for the hash
    public ArrayList<Node> getNodesFromHash(String key) {
        int mapping = Math.abs(key.hashCode()) % this.hashNum;
        ArrayList<Node> mappedNodes = new ArrayList<>();

        try {
            Integer first = this.hashRing.ceilingKey(mapping);
            if (first == null) {
                first = this.hashRing.ceilingKey(0);
            }
            Integer second = this.hashRing.higherKey(first);
            if (second == null) {
                second = this.hashRing.ceilingKey(0);
            }

            if (first != null) {
                mappedNodes.add(this.hashRing.get(first));
            }
            if (second != null) {
                mappedNodes.add(this.hashRing.get(second));
            }

        } catch (Exception e) {
            System.out.println(e);
        }

        return mappedNodes;
    }

    static class TransferRequest {
        private final String host;
        private final int port;
        private final String request;

        public TransferRequest(String host, int port, String request) {
            this.host = host;
            this.port = port;
            this.request = request;
        }

        public void send() {
            try (Socket node = new Socket(host, port)) {
                OutputStream output = node.getOutputStream();
                PrintWriter p = new PrintWriter(output, true);
                p.println(this.request);
                p.flush();
                // Wait for ack?
            } catch (Exception e) {
            }
        }
    }

    public void addNode(String host, int port) {
        Node newNode = new Node(host, port, "");
        if (this.nodes.contains(newNode)) {
            return;
        }

        System.out.println("Adding node " + host + ":" + port);

        ArrayList<Integer> keyList = new ArrayList<Integer>(this.hashRing.keySet());
        if (keyList.isEmpty() || this.nodes.isEmpty()) { // No nodes
            int rangeSize = this.hashNum/vnodes;
            int key = rangeSize;
            for (int i = 0; i < vnodes; i++) {
                this.hashRing.put(key, newNode);
                key += rangeSize;
            }
            this.nodes.add(newNode);
            this.debug();
            return;
        }

        Random random = new Random();
        ArrayList<TransferRequest> transferRequests = new ArrayList<TransferRequest>();
        try {
            for (int i = 0; i < vnodes; i++) {
                // Randomly pick an existing virtual node to split the range with
                int index = random.nextInt(keyList.size());
                int vnode = keyList.get(index);
                keyList.remove(index); // Remove so we don't pick the same vnode
                Node node = this.hashRing.get(vnode); // Get the node associated with vnode
                Integer start = this.hashRing.lowerKey(vnode);
                if (start == null) {
                    start = 0; // Start of first vnode will always be 0
                }
                int newVNode = start + ((vnode - start) / 2);
                this.hashRing.put(newVNode, newNode);
                String request = "TRANSFER " + "/?start=" + start + "&finish=" + newVNode + "&targetIP=" + newNode.host + "&targetPort=" + newNode.port + " HTTP/1.1";
                System.out.println(request);
                TransferRequest tr = new TransferRequest(node.host, node.port, request);
                transferRequests.add(tr);
            }
            this.nodes.add(newNode);
            this.debug();
            // Send transfer requests
            for (TransferRequest request: transferRequests) {
                request.send();
            }
        } catch (Exception e) {
        }  
    }

    public void removeNode(String host, int port) {
        System.out.println("Removing node " + host + ":" + port);
        // Remove node
        for (Node node: this.nodes) {
            if (Objects.equals(node.host, host) && node.port == port) {
                this.nodes.remove(node);
                break;
            }
        }
        // Remove virtual nodes
        ArrayList<Integer> keyList = new ArrayList<Integer>(this.hashRing.keySet());
        for (Integer key: keyList) {
            if (Objects.equals(this.hashRing.get(key).host, host) && this.hashRing.get(key).port == port) {
                this.hashRing.remove(key);
            }
        }
    }

    
    public void debug() {
        // Print all nodes and their ranges
        for (Integer key: this.hashRing.keySet()) {
            Integer start = this.hashRing.lowerKey(key);
            if (start == null) {
                start = 0;
            }
            Node node = this.hashRing.get(key);
            System.out.println(node.host + ":" + node.port + " range:" + start + "-" + key);
        }
    }
}


public class SimpleProxyServer {

    static final int HASH_NUM = 120;
    static final int REPLICATION_FACTOR = 2;
    static final int VNODES_PER_NODE = 2;
    static final boolean REPLICATE = true;
    static final boolean CACHING = false;
    static final int MAX_THREADS = 16;
    static final int CACHE_LIMIT = 30000;
    static ArrayList<SimpleProxyThread> threads = new ArrayList<>();
    static BlockingQueue<Socket> clientQueue = new LinkedBlockingQueue<>();
    static ConcurrentHashMap<String, byte[]> cache = new ConcurrentHashMap<String, byte[]>();

    public static void main(String[] args) throws IOException, InterruptedException {
        int proxyPort;
        int monitorport;
        String host = "CSC409 URL Shortener";
         if (args.length < 2) {
             System.out.println("Usage: SimpleProxyServer <port> <monitorport>");
             return;
         }

        ArrayList<Node> nodes = new ArrayList<>();

        proxyPort = Integer.parseInt(args[0]);
        monitorport = Integer.parseInt(args[1]);

        System.out.println("Starting proxy for " + host + " on port " + proxyPort);
        try {
            // And start running the server
            runServer(host, proxyPort, monitorport, new ConsistentHashing(nodes, HASH_NUM, REPLICATION_FACTOR, VNODES_PER_NODE));
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    static class MonitorThread extends Thread {
        private ServerSocket ms;
        private ConsistentHashing consistentHashing;

        public  MonitorThread(ServerSocket ms, ConsistentHashing consistentHashing) {
            this.ms = ms;
            this.consistentHashing = consistentHashing;
        }

        public void run() {
            while (true) {
                Socket monitor = null;
                try {
                    monitor = ms.accept();
                    final InputStream streamFromMonitor = monitor.getInputStream();
                    final OutputStream streamToMonitor = monitor.getOutputStream();

                    BufferedReader reader = new BufferedReader(new InputStreamReader(streamFromMonitor));
                    String line = reader.readLine();
                    System.out.println(line);
                    String host;
                    int port;
                    Pattern addPattern = Pattern.compile("^ADD\\s+/\\?host=(\\S+)&port=(\\S+)\\s+(\\S+)$");
                    Matcher madd = addPattern.matcher(line);
                    if (madd.matches()) { // PUT request
                        host = madd.group(1);
                        port = Integer.parseInt(madd.group(2));
                        consistentHashing.addNode(host, port);
                        // Send ack
                         String ack = "ACK";
                         PrintWriter p = new PrintWriter(streamToMonitor);
                         p.println(ack);
                         p.flush();
                    } else {
                        Pattern delPattern = Pattern.compile("^DEL\\s+/\\?host=(\\S+)&port=(\\S+)\\s+(\\S+)$");
                        Matcher mdel = delPattern.matcher(line);
                        if (mdel.matches()) {
                            host = mdel.group(1);
                            port = Integer.parseInt(mdel.group(2));
                            consistentHashing.removeNode(host, port);
                            consistentHashing.debug();
                            // Send ack
                             String ack = "ACK";
                             PrintWriter p = new PrintWriter(streamToMonitor);
                             p.println(ack);
                             p.flush();
                        }
                    }
                    // No match
                    String res = "404 NO MATCH" + line;
                    PrintWriter p = new PrintWriter(streamToMonitor);
                    p.println(res);
                    p.flush();
                    p.close();
                } catch (Exception e) {
                    System.err.println(e);
                } finally {
                    try {
                        if (monitor != null) {
                            monitor.close();
                        }
                    } catch (IOException _) {
                    }
                }
            }
        }
    }


    public static void runServer(String host, int localport, int monitorport, ConsistentHashing consistentHashing) throws IOException {
        // Create a ServerSocket to listen for connections with
        ServerSocket ss = new ServerSocket(localport);
        ServerSocket ms = new ServerSocket(monitorport);

        MonitorThread monitor = new MonitorThread(ms, consistentHashing);
        monitor.start();

        Thread accepting = new Thread() { // Thread to accept incoming connections
            public void run() {
                try {
                    while (true) {
                        clientQueue.add(ss.accept());
                    }
                } catch (IOException _) {
                }
            }
        };
        accepting.start();
        
        while (true) {
            if (threads.size() < MAX_THREADS) {
                try {
                    Socket client = clientQueue.take(); // Blocks if queue is empty
                    SimpleProxyThread clientThread = new SimpleProxyThread(client, consistentHashing, REPLICATE);
                    threads.add(clientThread);
                    clientThread.start();
                } catch (Exception e) {
                }
            }

            // Remove any threads that are finished
            for (Iterator<SimpleProxyThread> iterator = threads.iterator(); iterator.hasNext(); ) {
                SimpleProxyThread thread = iterator.next();
                if (!thread.isAlive()) {
                    iterator.remove();
                }
            }
            
        }
        
    }
}

