import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.ArrayList;

public class URLShortnerDB {
	private static Connection connect(String url) {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url);
			/**
			 * pragma locking_mode=EXCLUSIVE;
			 * pragma temp_store = memory;
			 * pragma mmap_size = 30000000000;
			 **/
			String sql = """
			pragma synchronous = normal;
			pragma journal_mode = WAL;
			""";
			Statement stmt  = conn.createStatement();
			stmt.executeUpdate(sql);

		} catch (SQLException e) {
			System.out.println(e.getMessage());
        	}
		return conn;
	}

	private Connection conn=null;
	public URLShortnerDB(){ this("jdbc:sqlite:database.db"); }
	public URLShortnerDB(String url){ conn = URLShortnerDB.connect(url); }

			   
	public String find(String shortURL) {
		try {
			Statement stmt  = conn.createStatement();
			String sql = "SELECT longurl FROM bitly WHERE shorturl=?;";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1,shortURL);
			ResultSet rs = ps.executeQuery();

			if(rs.next()) return rs.getString("longurl");
			else return null; 

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return null;
	}

	public ArrayList<String> select(int hashNumStart, int hashNumEnd) {
		ArrayList <String> list = new ArrayList<>();
		try {
			Statement stmt  = conn.createStatement();
			String sql = "SELECT * FROM bitly WHERE hashnum >= ? AND hashnum <= ?;";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setInt(1,hashNumStart);
			ps.setInt(2,hashNumEnd);
			ResultSet rs = ps.executeQuery();
			while(rs.next()) {
				list.add(rs.getString("shorturl") + "," + rs.getString("longurl") + "," + rs.getInt("hashnum"));
			}
			return list;
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return null;
	}

	public boolean insert(ArrayList<String> lines) {

		try {
			StringBuilder insertSQL = new StringBuilder("INSERT INTO bitly (shorturl, longurl, hashnum) VALUES ");
			String[] placeholders = new String[lines.size()];
			for (int i = 0; i < lines.size(); i++) {
				placeholders[i] = "(?, ?, ?)";
			}
			insertSQL.append(String.join(", ", placeholders)).append(" ON CONFLICT DO NOTHING;");

			PreparedStatement ps = conn.prepareStatement(insertSQL.toString());

			int index = 1;
			for (String line : lines) {
				String[] split = line.split(",");
				ps.setString(index++, split[0]); // shorturl
				ps.setString(index++, split[1]); // longurl
				ps.setInt(index++, Integer.parseInt(split[2])); // hashnum
			}

			ps.execute();
			return true;

		} catch (SQLException e) {
			System.out.println(e.getMessage());
			return false;
		}
	}

	public boolean save(String shortURL,String longURL, int hashNum){
		// System.out.println("shorturl="+shortURL+" longurl="+longURL);
		try {
			String insertSQL = "INSERT INTO bitly(shorturl,longurl,hashnum) VALUES(?,?,?) ON CONFLICT(shorturl) DO UPDATE SET longurl=?, hashnum=?;";
			PreparedStatement ps = conn.prepareStatement(insertSQL);
			ps.setString(1, shortURL);
			ps.setString(2, longURL);
			ps.setInt(3, hashNum);
			ps.setString(4, longURL);
			ps.setInt(5, hashNum);
			ps.execute();

			return true;

		} catch (SQLException e) {
			System.out.println(e.getMessage());
			return false;
		}
	}
}
