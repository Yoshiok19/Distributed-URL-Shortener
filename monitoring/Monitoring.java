import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ProxyServer {
    public final String host;
    public final int port;
    public boolean alive = true;

    public final int monitorPort;

    public final String processId;
    public final String pcHost;

    public ProxyServer(String pcHost, String host, int port, int monitorPort, String processId) {
        this.pcHost = pcHost;
        this.host = host;
        this.port = port;
        this.processId = processId;
        this.monitorPort = monitorPort;
    }
}

class Node {

    public final String pcHost;
    public final String host;
    public final int port;
    public boolean alive = true;

    public final String processId;

    public Node(String pcHost, String host, int port, String processId) {
        this.pcHost = pcHost;
        this.host = host;
        this.port = port;
        this.processId = processId;
    }

}

record Infrastructure(ProxyServer proxy, ArrayList<Node> nodes) {

    synchronized void addNode(Node node) {
        nodes.add(node);
    }

    synchronized boolean removeNode(String pcHost) {
        for (Node node : nodes) {
            if (node.pcHost.equals(pcHost)) {
                nodes.remove(node);
                return true;
            }
        }
        return false;
    }

    synchronized Node getNode(String pcHost) {
        for (Node node : nodes) {
            if (node.pcHost.equals(pcHost)) {
                return node;
            }
        }
        return null;
    }
    synchronized ArrayList<Node> getNodes() {
        return nodes;
    }

    synchronized ProxyServer getProxy() {
        return this.proxy;
    }
}


class MonitoringInputThread extends Thread {
    Socket socket;
    static final File WEB_ROOT = new File(".");
    static final String DEFAULT_FILE = "index.html";
    static final String FILE_NOT_FOUND = "404.html";
    static final String METHOD_NOT_SUPPORTED = "not_supported.html";
    static final String REDIRECT_RECORDED = "redirect_recorded.html";
    static final String REDIRECT = "redirect.html";
    static final String NOT_FOUND = "notfound.html";
    final Infrastructure infrastructure;

    public MonitoringInputThread (Socket connect, Infrastructure infra) {
        super();
        this.socket = connect;
        this.infrastructure = infra;
    }
    private static byte[] readFileData(File file, int fileLength) throws IOException {
        FileInputStream fileIn = null;
        byte[] fileData = new byte[fileLength];

        try {
            fileIn = new FileInputStream(file);
            fileIn.read(fileData);
        } finally {
            if (fileIn != null)
                fileIn.close();
        }

        return fileData;
    }

    public void run() {
        BufferedReader in = null; PrintWriter out = null; BufferedOutputStream dataOut = null;
        try {
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream());
            dataOut = new BufferedOutputStream(socket.getOutputStream());

            String input = in.readLine();

            Pattern padd = Pattern.compile("^ADD\\s+\\?host=(\\S+)&port=(\\d+)");
            Matcher madd = padd.matcher(input);

            Pattern pdelete = Pattern.compile("^DELETE\\s+\\?host=(\\S+)");
            Matcher mdelete = pdelete.matcher(input);

            Pattern pkill = Pattern.compile("^KILL");
            Matcher mkill = pkill.matcher(input);

            if (mkill.matches()) {
                ProxyServer proxy = infrastructure.getProxy();
                Process p = new ProcessBuilder("./kill.sh", proxy.pcHost, proxy.processId).start();
                p.waitFor();

                ArrayList<Node> nodes = infrastructure.getNodes();
                for (Node node : nodes) {
                    p = new ProcessBuilder("./kill.sh", node.pcHost, node.processId).start();
                    p.waitFor();
                    infrastructure.removeNode(node.pcHost);
                }
                System.exit(0);
            }
            else if (madd.matches()) {
                String pcHost = madd.group(1);
                String nodePort = madd.group(2);

                Process p = new ProcessBuilder("./get_node.sh", pcHost, nodePort).start();
                p.waitFor();
                BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String pid = reader.readLine();
                String nodeIP = reader.readLine();
                reader.close();
                Node node = new Node(pcHost, nodeIP, Integer.parseInt(nodePort), pid);
                infrastructure.addNode(node);
                try {
                    ProxyServer proxy = infrastructure.getProxy();
                    Socket dest = new Socket(proxy.host, proxy.monitorPort);
                    final InputStream streamFromDestination = dest.getInputStream();
                    final OutputStream streamToDestination = dest.getOutputStream();

                    String request = "ADD /?host=" + nodeIP + "&port=" + nodePort + " HTTP/1.1";

                    PrintWriter writer = new PrintWriter(streamToDestination);
                    writer.println(request);
                    writer.flush();
                    // TODO: use streamFromDestination to read response
                    BufferedReader reader1 = new BufferedReader(new InputStreamReader(streamFromDestination));
                    String line = reader1.readLine();
                    if (line.equals("ACK")) {
                        System.out.println("Node added successfully");
                        out.println("HTTP/1.1 200");
                    }
                    else {
                        System.out.println("Reponse: " + line);
                        System.out.println("Node addition failed?");
                        out.println("HTTP/1.1 400 Not Available");
                    }
                    dest.close();
                }
				catch (IOException e) {
                    System.out.println(e.getMessage());
                    out.println("HTTP/1.1 400 Not Available");
                }
            }
            else if(mdelete.matches()) {
                String pcHost = mdelete.group(1);

                Node node = infrastructure.getNode(pcHost);

                if (node == null) {
                    return;
                }
                try {
                    ProxyServer proxy = infrastructure.getProxy();
                    Socket dest = new Socket(proxy.host, proxy.monitorPort);
                    final InputStream streamFromDestination = dest.getInputStream();
                    final OutputStream streamToDestination = dest.getOutputStream();

                    String request = "DELETE /?host=" + node.host + "&port=" + node.port;

                    PrintWriter writer = new PrintWriter(streamToDestination);
                    writer.println(request);
                    writer.flush();
                    BufferedReader reader1 = new BufferedReader(new InputStreamReader(streamFromDestination));
                    String line = reader1.readLine();
                    if (line.equals("ACK")) {
                        System.out.println("Node added successfully");
                        out.println("HTTP/1.1 200");
                    }
                    else {
                        System.out.println("Node addition failed?");
                        out.println("HTTP/1.1 400 Not Available");
                    }
                    dest.close();
                    // TODO: use streamFromDestination to read response
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                    out.println("HTTP/1.1 400 Not Available");
                }

                Process p = new ProcessBuilder("./kill.sh", pcHost, node.processId).start();
                p.waitFor();
                infrastructure.removeNode(pcHost);
            }
        } catch (Exception e) {
            System.err.println(e.toString());
            System.err.println("Server error");
        } finally {
            try {
                in.close();
                out.close();
                this.socket.close(); // we close socket connection
            } catch (Exception e) {
                System.err.println("Error closing stream : " + e.getMessage());
            }
            System.out.println("Connection closed.\n");
        }
    }
}

class PingThread extends Thread {
    final Infrastructure infrastructure;

    public PingThread(Infrastructure infrastructure) {
        super();
        this.infrastructure = infrastructure;
    }

    public void run() {
        while (true) {
            for (Node node: infrastructure.getNodes()) {
                try {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress(node.host, node.port), 5000);
                    socket.close();
                    System.out.println("Node " + node.pcHost + " " + node.host + ":" + node.port + " is up" + " pid " + node.processId);
                } catch (IOException e) {
                    System.out.println("Node " + node.pcHost + " " + node.host + ":" + node.port + " is down" + " pid " + node.processId);

                    for (int i = 0; i < 5; i++) {
                        try {
                            ProxyServer proxy = infrastructure.getProxy();
                            Socket dest = new Socket(proxy.host, proxy.monitorPort);
                            final InputStream streamFromDestination = dest.getInputStream();
                            final OutputStream streamToDestination = dest.getOutputStream();

                            String request = "DEL /?host=" + node.host + "&port=" + node.port + " HTTP/1.1";
                            PrintWriter writer = new PrintWriter(streamToDestination);
                            writer.println(request);
                            writer.flush();

                            BufferedReader reader = new BufferedReader(new InputStreamReader(streamFromDestination));
                            String line = reader.readLine();
                            System.out.println("Reponse: " + line);
                            if (line.equals("ACK")) {
                                break;
                            }
                            dest.close();
                        }
                        catch (IOException ex) {
                            System.out.println(ex.getMessage());
                            System.out.println("Can't establish connection with proxy");
                            try {
                                sleep(1000);
                            } catch (InterruptedException exc) {
                                throw new RuntimeException(exc);
                            }
                            continue;
                        }
                        break;
                    }
                    infrastructure.removeNode(node.pcHost);

                    break;
                }
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println("Exiting ping thread...");
                    return;
                }
            }
        }
    }
}

class ShutDownHook extends Thread {
    final Infrastructure infrastructure;

    public ShutDownHook(Infrastructure infrastructure) {
        super();
        this.infrastructure = infrastructure;
    }

    public void run() {
        try {
            System.out.println("KILLING PROXY");
            ProxyServer proxy = infrastructure.getProxy();
            Process p = new ProcessBuilder("./kill.sh", proxy.pcHost, proxy.processId).start();
            p.waitFor();
            p.destroy();
            ArrayList<Node> nodes = infrastructure.getNodes();
            ArrayList<Node> nodesCopy = new ArrayList<Node>();

            System.out.println("KILLED PROXY");

            for (Node node : nodes) {
                nodesCopy.add(node);
            }

            for (Node node : nodesCopy) {
                p = new ProcessBuilder("./kill.sh", node.pcHost, node.processId).start();
                p.waitFor();
                p.destroy();
                System.out.println("killed node "+ node.pcHost);
            }

        }
        catch (Exception e) {
            System.out.println("Error in shutting down");
        }

    }
}

public class Monitoring {

    public static void main(String[] args) throws IOException, InterruptedException {

        ArrayList<Node> nodes = new ArrayList<Node>();
        String proxyHost;
        String proxyPort;
        String proxyMonitorPort;
        int monitorport;

        if (args.length < 6 || (args.length % 2 != 0)) {
            System.out.println("Usage: Monitoring <proxy dh2020pcXX> <proxy port> <proxy monitor port> <monitorport> <node1 dh2020pcYY> <node1 port> ...");
            return;
        }
        proxyHost = args[0];
        proxyPort = args[1];
        proxyMonitorPort =args[2];
        monitorport = Integer.parseInt(args[3]);

        Process p = new ProcessBuilder("./get_proxy.sh", proxyHost, proxyPort, proxyMonitorPort).start();
        p.waitFor();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String pid = reader.readLine();
        String nodeIP = reader.readLine();
        reader.close();
        ProxyServer proxy = new ProxyServer(proxyHost, nodeIP, Integer.parseInt(proxyPort), Integer.parseInt(proxyMonitorPort), pid);
        System.out.println("Proxy: " + nodeIP + ":" + proxyPort + " Monitor Port: " + proxyMonitorPort + " pid: " + pid);

        for (int i = 4; i < args.length; i+=2) {
            p = new ProcessBuilder("./get_node.sh", args[i], args[i+1]).start();
            p.waitFor();
            reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            pid = reader.readLine();
            nodeIP = reader.readLine();
            reader.close();
            nodes.add(new Node(args[i], nodeIP, Integer.parseInt(args[i+1]), pid));
            System.out.println("Node " + nodeIP + ":" + args[i+1] + " pid: " + pid);

            try {
                Socket dest = new Socket(proxy.host, proxy.monitorPort);
                final InputStream streamFromDestination = dest.getInputStream();
                final OutputStream streamToDestination = dest.getOutputStream();

                String request = "ADD /?host=" + nodeIP + "&port=" + Integer.parseInt(args[i+1]) + " HTTP/1.1";

                PrintWriter writer = new PrintWriter(streamToDestination);

                writer.println(request);
                writer.flush();
                BufferedReader reader1 = new BufferedReader(new InputStreamReader(streamFromDestination));
                String line = reader1.readLine();
                if (line.equals("ACK")) {
                    System.out.println("Reponse: " + line);
                    System.out.println("Node added successfully");
                }
                else {
                    System.out.println("Reponse: " + line);
                    System.out.println("Node addition failed?");
                }
                dest.close();
                // TODO: use streamFromDestination to read response
            }
            catch (Exception e) {
                System.out.println(e.getMessage());
                System.out.println("Can't establish connection with proxy");
            }
        }
        Infrastructure infra = new Infrastructure(proxy, nodes);
        Runtime.getRuntime().addShutdownHook(new ShutDownHook(infra));

        spinUp(monitorport, infra);
    }

    public static void spinUp(int monitorPort, Infrastructure infra) throws IOException {
        PingThread pingThread = new PingThread(infra);
        pingThread.start();
        ServerSocket serverConnect = new ServerSocket(monitorPort);
        while (true) {
            handleIncomingRequest(serverConnect.accept(),infra);
        }
    }

    public static void handleIncomingRequest(Socket connect, Infrastructure infra) {
        MonitoringInputThread handler = new MonitoringInputThread(connect, infra);
        handler.start();
    }
}
