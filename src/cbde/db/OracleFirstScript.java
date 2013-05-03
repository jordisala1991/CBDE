package cbde.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleFirstScript {
	
	private Connection connection;

	private static final String URL = "jdbc:oracle:thin:@oraclefib.fib.upc.es:1521:ORABD";
	private static final String USERNAME = "daniel.llamazares";
	private static final String PASSWORD = "DB021091";
	
	public OracleFirstScript() throws SQLException {
	
		connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
	}
	
}
