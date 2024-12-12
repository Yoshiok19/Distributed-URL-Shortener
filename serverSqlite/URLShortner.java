import java.io.*;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


class URLShortnerHandler extends Thread {
	Socket socket;
	URLShortnerDB database;
	static final File WEB_ROOT = new File(".");
	static final String DEFAULT_FILE = "index.html";
	static final String FILE_NOT_FOUND = "404.html";
	static final String METHOD_NOT_SUPPORTED = "not_supported.html";
	static final String REDIRECT_RECORDED = "redirect_recorded.html";
	static final String REDIRECT = "redirect.html";
	static final String NOT_FOUND = "notfound.html";

	public URLShortnerHandler(Socket connect, URLShortnerDB database) {
		super();
		this.socket = connect;
		this.database = database;
	}

	public synchronized void saveToDB(String shortResource, String longResource, int hashNum) {
		database.save(shortResource, longResource, hashNum);
	}

	public synchronized ArrayList<String> fetchFromDB(int hashNumStart, int hashNumEnd) {
		return database.select(hashNumStart, hashNumEnd);
	}

	public synchronized boolean bulkInsertIntoDb(ArrayList<String> lines) {
		return database.insert(lines);
	}

	public synchronized String GetFromDb(String shortResource) {
		return database.find(shortResource);
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

			Pattern pput = Pattern.compile("^PUT\\s+/\\?short=(\\S+)&long=([^&]+?)&hashnum=(\\S+)\\s+(\\S+)$");
			Matcher mput = pput.matcher(input);

			Pattern ptransfer = Pattern.compile("^TRANSFER\\s+/\\?start=(\\d+)&finish=(\\d+)&targetIP=(\\S+)&targetPort=(\\S+)\\s+(\\S+)$");
			Matcher mtransfer = ptransfer.matcher(input);

			Pattern pbulk = Pattern.compile("^BULK$");
			Matcher mbulk = pbulk.matcher(input);
			System.out.println("RECEIVED LINE: " + input);
			if (mbulk.matches()) {
				ArrayList<String> lines = new ArrayList<>();
				while(in.ready()) {
					lines.add(in.readLine());
				}
				bulkInsertIntoDb(lines);

				File file = new File(WEB_ROOT, REDIRECT_RECORDED);
				int fileLength = (int) file.length();
				String contentMimeType = "text/html";
				//read content to return to client
				byte[] fileData = readFileData(file, fileLength);
				out.println("HTTP/1.1 200 OK");
				out.println("Server: Java HTTP Server/Shortner : 1.0");
				out.println("Date: " + new Date());
				out.println("Content-type: " + contentMimeType);
				out.println("Content-length: " + fileLength);
				out.println();
				out.flush();

				dataOut.write(fileData, 0, fileLength);
				dataOut.flush();
			}
			else if (mtransfer.matches()) {
				int startHashNum = Integer.parseInt(mtransfer.group(1));
				int endHashNum = Integer.parseInt(mtransfer.group(2));
				String targetIp = mtransfer.group(3);
				int targetPort = Integer.parseInt(mtransfer.group(4));
				ArrayList<String> list = fetchFromDB(startHashNum, endHashNum);
				try {
					Socket dest = new Socket(targetIp, targetPort);
					final InputStream streamFromDestination = dest.getInputStream();
					final OutputStream streamToDestination = dest.getOutputStream();

					PrintWriter writer = new PrintWriter(streamToDestination);
					writer.println("BULK");
					writer.print(String.join("\n", list));
					writer.flush();
					dest.close();
				}
				catch (IOException e) {

				}
			}
			else if(mput.matches()){
				String shortResource=mput.group(1);
				String longResource=mput.group(2);
				int hashNum=Integer.parseInt(mput.group(3));
				String httpVersion=mput.group(4);
				saveToDB(shortResource, longResource, hashNum);

				File file = new File(WEB_ROOT, REDIRECT_RECORDED);
				String contentMimeType = "text/html";
				//read content to return to client

				out.println("HTTP/1.1 200 OK");
				out.println("Server: Java HTTP Server/Shortner : 1.0");
				out.println("Date: " + new Date());
				out.println("Content-type: " + contentMimeType);
				out.println();
				out.flush();
			} else {
				Pattern pget = Pattern.compile("^(\\S+)\\s+/(\\S+)\\s+(\\S+)$");
				Matcher mget = pget.matcher(input);
				if(mget.matches()){
					String method=mget.group(1);
					String shortResource=mget.group(2);
					String httpVersion=mget.group(3);

					String longResource = this.GetFromDb(shortResource);

					if(longResource!=null){
						File file = new File(WEB_ROOT, REDIRECT);
						int fileLength = (int) file.length();
						String contenclasspathtMimeType = "text/html";

						//read content to return to client
						byte[] fileData = readFileData(file, fileLength);
						String contentMimeType = "text/html";
						// out.println("HTTP/1.1 301 Moved Permanently");
						out.println("HTTP/1.1 307 Temporary Redirect");
						out.println("Location: "+longResource);
						out.println("Server: Java HTTP Server/Shortner : 1.0");
						out.println("Date: " + new Date());
						out.println("Content-type: " + contentMimeType);
						out.println("Content-length: " + fileLength);
						out.println();
						out.flush();

						dataOut.write(fileData, 0, fileLength);
						dataOut.flush();
					} else {
						File file = new File(WEB_ROOT, FILE_NOT_FOUND);
						int fileLength = (int) file.length();
						String content = "text/html";
						byte[] fileData = readFileData(file, fileLength);
						out.println("HTTP/1.1 404 File Not Found");
						out.println("Server: Java HTTP Server/Shortner : 1.0");
						out.println("Date: " + new Date());
						out.println("Content-type: " + content);
						out.println("Content-length: " + fileLength);
						out.println();
						out.flush();

						dataOut.write(fileData, 0, fileLength);
						dataOut.flush();
					}
				}
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

public class URLShortner { 

	static URLShortnerDB database=null;
	// port to listen connection
	static int PORT;
	static ArrayList<URLShortnerHandler> handlers = new ArrayList<>();

	// verbose mode
	static final boolean verbose = true;

	public static void main(String[] args) {
		String db_path = "jdbc:sqlite:/virtual/a1group09/";
		if (args.length < 2) {
			if (verbose) System.out.println("Usage: URLShortner <port> <hostname>");
			return;
		}
		try {
			PORT = Integer.parseInt(args[0]);
			db_path += args[1] + ".db";
		}
		catch (Exception e) {
			if (verbose) System.out.println("Usage: URLShortner <port>");
			return;
		}

		database = new URLShortnerDB(db_path);
		try {
			ServerSocket serverConnect = new ServerSocket(PORT);
			if (verbose)System.out.println("Server started.\nListening for connections on port : " + PORT + " ...\n");
			
			// we listen until user halts server execution
			while (true) {
				if (verbose)System.out.println("Connecton opened. (" + new Date() + ")");
				handle(serverConnect.accept());
			}
		} catch (IOException e) {
			if (verbose)System.err.println("Server Connection error : " + e.getMessage());
		}
	}

	public static void handle(Socket connect) {
		URLShortnerHandler handler = new URLShortnerHandler(connect, database);
		handlers.add(handler);
		handler.start();
	}


}
